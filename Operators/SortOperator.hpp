//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_SORTOPERATOR_HPP
#define OLVP_SORTOPERATOR_HPP


#include "../Operators/Operator.hpp"
#include "arrow/compute/api_vector.h"
#include "arrow/table.h"
#include "../Page/Channel.hpp"
#include "../Utils/ArrowArrayBuilder.hpp"
#include "../Execution/Task/Context/DriverContext.h"
class SortOperator:public Operator
{


public:
    enum SortOrder
    {
        Ascending,
        Descending
    };
private:
    bool finished;

    string name = "SortOperator";




    bool sendEndPage = false;



    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;


    std::shared_ptr<arrow::Schema> schema = NULL;




    vector<Channel> channels;

    vector<string> sortKeys;
    vector<SortOrder> sortOrders;

    shared_ptr<DriverContext> driverContext;

public:



    string getOperatorId() { return this->name; }

    SortOperator(shared_ptr<DriverContext> driverContext,vector<string> sortKeys,vector<SortOrder> sortOrders) {

        this->sortKeys = sortKeys;
        this->sortOrders = sortOrders;
        this->driverContext = driverContext;
        this->finished = false;

    }


    void addInput(std::shared_ptr<DataPage> input) override {
        if(input != NULL && !input->isEndPage()) {
            this->inputPage = input;
            if(this->schema == NULL) {
                this->schema = this->inputPage->get()->schema();

                for(int i = 0 ; i <  this->inputPage->get()->schema()->num_fields() ; i++)
                    this->channels.push_back(Channel());
            }

            for(int i = 0 ; i < this->inputPage->get()->num_columns() ; i++)
                this->channels[i].addChunk(this->inputPage->get()->column(i));
        }
        else
        {
            this->inputPage = input;
        }

    }


    void process() {

        if(this->inputPage->getElementsCount() == 0 || this->inputPage->getElementsCount() == -1) {
            this->inputPage = NULL;
            return;
        }

        vector<std::shared_ptr<arrow::ChunkedArray>> chunkedArrays;
        for (int i = 0; i < this->channels.size(); i++)
        {
            chunkedArrays.push_back(this->channels[i].toChunkedArray());
        }
        std::shared_ptr<arrow::Table> table = arrow::Table::Make(this->schema,chunkedArrays);


        vector<arrow::compute::SortKey> arrowSortKeys;

        for(int i = 0 ; i < this->sortKeys.size() ; i++)
        {
            if(this->sortOrders[i] == Descending) {
                arrow::compute::SortKey sK(this->sortKeys[i], arrow::compute::SortOrder::Descending);
                arrowSortKeys.push_back(sK);
            }
            else {
                arrow::compute::SortKey sK(this->sortKeys[i], arrow::compute::SortOrder::Ascending);
                arrowSortKeys.push_back(sK);
            }
        }
        arrow::compute::SortOptions options(arrowSortKeys);

        arrow::Result<shared_ptr<arrow::Array>> result = arrow::compute::SortIndices(table,options);

        if(result.ok())
        {
            std::shared_ptr<arrow::Array> arrays = result.ValueOrDie();
            auto indices = std::static_pointer_cast<arrow::Int64Array>(arrays);


            vector<ArrowArrayBuilder> builders;
            for(int i = 0 ; i < table->num_columns() ; i++)
            {
                builders.push_back(ArrowArrayBuilder(table->column(i)->type()));
            }

            for(int i = 0 ; i < table->num_columns() ; i++)
            {
                for(int j = 0 ; j < table->column(i)->length() ; j++)
                {
                    builders[i].addScalar(table->column(i)->GetScalar(indices->Value(j)).ValueOrDie());
                }
            }


            vector<std::shared_ptr<arrow::Array>> sortedResult;

            for(int i = 0 ; i < builders.size() ; i++)
            {
                sortedResult.push_back(builders[i].buildFinish().ValueOrDie());
            }

            this->outPutPage = std::make_shared<DataPage>(arrow::RecordBatch::Make(this->schema,indices->length(),sortedResult));

        }
        else
        {
            spdlog::critical("Sort operator get indices failed!");
            return;
        }




    }


    std::shared_ptr<DataPage> getOutput() override {



        if(this->sendEndPage)
        {
            this->finished = true;
            return DataPage::getEndPage();
        }

        if(this->inputPage == NULL)
            return NULL;

        if(this->inputPage->isEndPage()) {
            this->sendEndPage = true;
            process();
            return this->outPutPage;
        }


        return NULL;

    }


    bool needsInput() override {
            return true;

    }


    bool isFinished()
    {
        return this->finished;
    }


};




#endif //OLVP_SORTOPERATOR_HPP
