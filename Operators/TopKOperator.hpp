//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_TOPKOPERATOR_HPP
#define OLVP_TOPKOPERATOR_HPP

#include "../Operators/Operator.hpp"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/table.h"
#include "../Page/Channel.hpp"
#include "../Utils/ArrowArrayBuilder.hpp"



class TopKOperator:public Operator
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

    vector<string> sortKeys;
    vector<SortOrder> sortOrders;
    vector<arrow::compute::SortKey> arrowSortKeys;
    std::shared_ptr<arrow::compute::SelectKOptions> options;
    vector<shared_ptr<arrow::Array>> TopKResult;

    shared_ptr<DriverContext> driverContext;


public:



    string getOperatorId() { return this->name; }

    TopKOperator(shared_ptr<DriverContext> driverContext,int64_t k,vector<string> sortKeys,vector<SortOrder> sortOrders) {

        this->sortKeys = sortKeys;
        this->sortOrders = sortOrders;
        this->driverContext = driverContext;
        this->finished = false;


        for(int i = 0 ; i < this->sortKeys.size() ; i++)
        {
            if(this->sortOrders[i] == Descending) {
                arrow::compute::SortKey sK(this->sortKeys[i], arrow::compute::SortOrder::Descending);
                this->arrowSortKeys.push_back(sK);
            }
            else {
                arrow::compute::SortKey sK(this->sortKeys[i], arrow::compute::SortOrder::Ascending);
                this->arrowSortKeys.push_back(sK);
            }
        }

        this->options = std::make_shared<arrow::compute::SelectKOptions>(k,this->arrowSortKeys);




    }


    void addInput(std::shared_ptr<DataPage> input) override {
        if(input != NULL && !input->isEndPage()) {
            this->inputPage = input;
            if(this->schema == NULL) {
                this->schema = this->inputPage->get()->schema();
            }
        }
        else
        {
            this->inputPage = input;
        }

    }


    void process() {

        if(this->inputPage->getElementsCount() == 0) {
            this->inputPage = NULL;
            return;
        }

        vector<Channel> channels;
        vector<std::shared_ptr<arrow::ChunkedArray>> chunkedArrays;
        for(int i = 0 ; i < this->inputPage->get()->num_columns() ; i++)
        {
            Channel ch;
            ch.addChunk(this->inputPage->get()->column(i));
            if(this->TopKResult.size() > 0)
                ch.addChunk(this->TopKResult[i]);

            channels.push_back(ch);

            chunkedArrays.push_back(channels[i].toChunkedArray());
        }

        std::shared_ptr<arrow::Table> table = arrow::Table::Make(this->schema,chunkedArrays);

        arrow::Result<shared_ptr<arrow::Array>> result = arrow::compute::SelectKUnstable(arrow::Datum(table),*this->options);


        if(!result.ok()){
            spdlog::critical("Sort operator get indices failed!");
            return;
        }

        auto indices = std::static_pointer_cast<arrow::Int64Array>(result.ValueOrDie());

        vector<ArrowArrayBuilder> builders;
        for(int i = 0 ; i < table->num_columns() ; i++)
        {
            builders.push_back(ArrowArrayBuilder(table->column(i)->type()));
        }

        for(int i = 0 ; i < table->num_columns() ; i++)
        {
            for(int j = 0 ; j < indices->length() ; j++)
            {
                builders[i].addScalar(table->column(i)->GetScalar(indices->Value(j)).ValueOrDie());
            }
        }

        vector<std::shared_ptr<arrow::Array>> topkResult;

        for(int i = 0 ; i < builders.size() ; i++)
        {
            topkResult.push_back(builders[i].buildFinish().ValueOrDie());
        }


        this->TopKResult = topkResult;
        this->inputPage = NULL;

    }

    void produceOutput()
    {
        if(this->schema == NULL || this->TopKResult.empty())
            this->outPutPage = DataPage::getEndPage();
        else
            this->outPutPage = std::make_shared<DataPage>(arrow::RecordBatch::Make(this->schema,this->TopKResult[0]->length(),this->TopKResult));
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
            this->produceOutput();
            return this->outPutPage;
        }
        else
        {
            process();
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




#endif //OLVP_TOPKOPERATOR_HPP
