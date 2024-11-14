//
// Created by zxk on 6/28/23.
//

#ifndef OLVP_NESTEDLOOPJOINOPERATOR_HPP
#define OLVP_NESTEDLOOPJOINOPERATOR_HPP



#include "../Operators/Operator.hpp"
#include "Join/NestedLoopJoin/NestedLoopJoinPagesSupplier.hpp"
#include "../Execution/Task/Context/DriverContext.h"
#include "arrow/api.h"


class NestedLoopJoinOperator :public Operator{



    std::shared_ptr <arrow::Schema> probeSchema = NULL;
    std::shared_ptr <arrow::Schema> buildSchema = NULL;
    std::shared_ptr <arrow::Schema> probeOutputSchema = NULL;
    std::shared_ptr <arrow::Schema> buildOutputSchema = NULL;

    class BuildPageIterator
    {
        vector<shared_ptr<DataPage>> pages;
        int curIndex = 0;
    public:
        BuildPageIterator(shared_ptr<NestedLoopJoinPages> buildPages)
        {
            this->pages = buildPages->getPages();
        }
        bool hasNext()
        {
            return curIndex < this->pages.size();
        }
        shared_ptr<DataPage> next()
        {
            shared_ptr<DataPage> page = this->pages[curIndex];
            this->curIndex++;
            return page;
        }

    };

    class NestedLoopOutputIterator
    {
    public:
        virtual bool hasNext() = 0;
        virtual shared_ptr<DataPage> next() = 0;
    };

    class PageRepeatingIterator:public NestedLoopOutputIterator
    {
        shared_ptr<DataPage> page;
        int remainingCount;

    public:
        PageRepeatingIterator(shared_ptr<DataPage> page, int repetitions)
        {
            this->page = page;
            this->remainingCount = repetitions;
        }


        bool hasNext()
        {
            return remainingCount > 0;
        }

        shared_ptr<DataPage> next()
        {
            if (!hasNext()) {
                spdlog::critical("No such element!");
            }
            remainingCount--;
            return page;
        }

    };


    class NestedLoopPageBuilder: public NestedLoopOutputIterator {
//  Avoids allocation a new block array per iteration
        vector<shared_ptr<arrow::Array>> resultBlockBuffer;
        shared_ptr<DataPage> smallPage;
        int indexForAddBlocks;
        int largePagePositionCount;
        int maxRowIndex; // number of rows - 1
        int rowIndex = -1; // Iterator on the rows in the page with less rows.
        std::shared_ptr <arrow::Schema> outputSchema;
    public:
        NestedLoopPageBuilder(shared_ptr<DataPage> probePage, shared_ptr<DataPage> buildPage,std::shared_ptr <arrow::Schema> probeOutputSchema,std::shared_ptr <arrow::Schema> buildOutputSchema) {

            shared_ptr<DataPage> largePage;
            int indexForPageBlocks;
            if (buildPage->getElementsCount() > probePage->getElementsCount()) {
                largePage = buildPage;
                smallPage = probePage;

                vector<std::shared_ptr<arrow::Field>> fields;



                for(int i = 0 ; i < probeOutputSchema->num_fields() ; i++)
                    fields.push_back(probeOutputSchema->field(i));
                for(int i = 0 ; i < buildOutputSchema->num_fields() ; i++)
                    fields.push_back(buildOutputSchema->field(i));

                this->outputSchema = arrow::schema(fields);


                indexForPageBlocks = probePage->get()->num_columns();
                this->indexForAddBlocks = 0;
            } else {
                largePage = probePage;
                smallPage = buildPage;
                indexForPageBlocks = 0;
                this->indexForAddBlocks = probePage->get()->num_columns();

                vector<std::shared_ptr<arrow::Field>> fields;
                for(int i = 0 ; i < probeOutputSchema->num_fields() ; i++)
                    fields.push_back(probeOutputSchema->field(i));
                for(int i = 0 ; i < buildOutputSchema->num_fields() ; i++)
                    fields.push_back(buildOutputSchema->field(i));
                this->outputSchema = arrow::schema(fields);


            }
            largePagePositionCount = largePage->getElementsCount();
            maxRowIndex = smallPage->getElementsCount() - 1;

            resultBlockBuffer.reserve(largePage->get()->num_columns() + smallPage->get()->num_columns());
            for(int i = 0 ; i < largePage->get()->num_columns() + smallPage->get()->num_columns() ; i++)
            {
                resultBlockBuffer.push_back(NULL);
            }

            for (int i = 0; i < largePage->get()->num_columns();i++) {
                resultBlockBuffer[indexForPageBlocks + i] = largePage->get()->column(i);
            }

            cout << outputSchema->ToString()<<endl;
        }



        bool hasNext() {
            return rowIndex < maxRowIndex;
        }


        shared_ptr<DataPage> next() {

            rowIndex++;


            for (int i = 0; i < smallPage->get()->num_columns(); i++) {
                shared_ptr<arrow::Array> array = smallPage->getSingleValueArray(i,rowIndex,largePagePositionCount);
                resultBlockBuffer[indexForAddBlocks + i] = array;
            }


            shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(this->outputSchema,largePagePositionCount,resultBlockBuffer);

            shared_ptr<DataPage> page = make_shared<DataPage>(batch);


            return page;
        }
    };


    bool finished;

    string name = "NestedLoopJoinOperator";


    bool sendEndPage = false;



    std::shared_ptr<NestedLoopJoinBridge> supplier = NULL;

    std::shared_ptr<NestedLoopJoinPages> buildPages = NULL;

    std::shared_ptr<BuildPageIterator> buildPageIterator = NULL;

    std::shared_ptr <DataPage> inputPage = NULL;
    std::shared_ptr <DataPage> outPutPage = NULL;


    shared_ptr<DataPage> probePage = NULL;

    shared_ptr<NestedLoopOutputIterator> nestedLoopOutputIterator = NULL;


    shared_ptr<DriverContext> driverContext;

    future<shared_ptr<NestedLoopJoinPages>> sourceFuture;
    int count = 0;
public:


    string getOperatorId() { return this->name; }

    NestedLoopJoinOperator(shared_ptr<DriverContext> driverContext,std::shared_ptr <arrow::Schema> probeSchema,std::shared_ptr <arrow::Schema> buildSchema, std::shared_ptr <arrow::Schema> probeOutputSchema,std::shared_ptr <arrow::Schema> buildOutputSchema,std::shared_ptr<NestedLoopJoinBridge>  supplier) {

        this->finished = false;

        this->probeSchema = probeSchema;
        this->buildSchema = buildSchema;
        this->buildOutputSchema = buildOutputSchema;
        this->probeOutputSchema = probeOutputSchema;


        this->supplier = supplier;
        this->driverContext = driverContext;

        this->sourceFuture = this->supplier->getPagesFuture();



    }

    void addInput(std::shared_ptr <DataPage> input) override {
        if (input != NULL && !input->isEndPage()) {

            if(!input->isEmptyPage())
            {

                this->inputPage = input;
                this->probePage = inputPage;
                this->buildPageIterator = make_shared<BuildPageIterator>(this->buildPages);
                this->count++;

            }

        } else {

            this->inputPage = input;
            spdlog::debug("crossjoin process "+to_string(count) + " pages.");
        }

    }

    bool tryFetchBuildPages()
    {
        if (this->buildPages == NULL) {

            this->supplier->tryGetCompletedPages();
            this->buildPages =  this->sourceFuture.get();

            return true;
        }

        return true;
    }


    int multiplyExact(int x, int y) {
        long r = (long)x * (long)y;
        if ((int)r != r) {
           spdlog::critical("interger overflow!");
        }
        return (int)r;
    }


    shared_ptr<NestedLoopOutputIterator> createNestedLoopOutputIterator(shared_ptr<DataPage> probePage,shared_ptr<DataPage> buildPage)
    {
        if (probePage->get()->num_columns() == 0 && buildPage->get()->num_columns() == 0) {
           return NULL;
        }
        else if (probePage->get()->num_columns() == 0 && probePage->getElementsCount() <= buildPage->getElementsCount()) {
            return NULL;
        }
        else if (buildPage->get()->num_columns() == 0 && buildPage->getElementsCount() <= probePage->getElementsCount()) {
            return NULL;
        }
        else {
            return make_shared<NestedLoopPageBuilder>(probePage, buildPage,probeOutputSchema,buildOutputSchema);
        }
    }




    std::shared_ptr <DataPage> getOutput() override {


        if(this->inputPage != NULL) {
            if (this->inputPage->isEndPage()) {
                this->sendEndPage = true;
            }
        }

        if (this->sendEndPage) {
            this->finished = true;
            return DataPage::getEndPage();
        }





        if(this->probePage == NULL | this->buildPages == NULL)
            return  NULL;


        if(this->nestedLoopOutputIterator != NULL && this->nestedLoopOutputIterator->hasNext())
        {
            return this->nestedLoopOutputIterator->next();
        }

        if(this->buildPageIterator->hasNext())
        {
            this->nestedLoopOutputIterator = createNestedLoopOutputIterator(this->probePage,this->buildPageIterator->next());
            return this->nestedLoopOutputIterator->next();
        }

        this->probePage = NULL;
        this->nestedLoopOutputIterator = NULL;


        return NULL;

    }


    bool needsInput() override {

        if(this->probePage != NULL || this->isFinished())
            return false;

        if(buildPages == NULL)
        {
            this->tryFetchBuildPages();
        }

        return buildPages != NULL;
    }


    bool isFinished() {
        return this->finished;
    }

};




#endif //OLVP_NESTEDLOOPJOINOPERATOR_HPP
