//
// Created by zxk on 5/26/23.
//

#ifndef OLVP_FINALAGGREGATIONOPERATOR_HPP
#define OLVP_FINALAGGREGATIONOPERATOR_HPP



#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"

#include "arrow/acero/options.h"
#include "arrow/acero/aggregate_node.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/table.h"


#include "../Operators/Operator.hpp"

#include "../Descriptor/AggregationDescriptor.hpp"
#include <future>

#include "../Utils/ArrowFunctionOptionsSupplier.hpp"
#include "tbb/concurrent_queue.h"
#include "../Utils/BlockQueue.hpp"


class FinalAggregationOperator:public Operator {

    bool finished;

    string name = "FinalAggregationOperator";


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;


    std::shared_ptr<arrow::Schema> input_schema;

    std::shared_ptr<arrow::Schema> output_schema;

    AggregationDesc desc;


    mutex resultLock;


    vector<arrow::acero::aggregate::Aggregate> aggregates;
    vector<arrow::FieldRef> groupByKeys;

    shared_ptr<arrow::Table> aggResult = NULL;
    shared_ptr<arrow::TableBatchReader> reader = NULL;


    shared_ptr<DriverContext> driverContext;

    bool sendEndPage = false;

    bool outputResultCompeleted = false;



    class DataPageTransfer
    {

        mutex lock;

        BlockQueue<shared_ptr<DataPage>> queue;

    public:

        DataPageTransfer(){

        }

        std::shared_ptr<DataPage> getPage()
        {
            return queue.Take();
        }


        bool givePage(std::shared_ptr<DataPage> page)
        {
           if(this->queue.Size() == 0) {
               this->queue.Put(page);
               return true;
           }
           else
               return false;
        }
    };


    std::shared_ptr<DataPageTransfer> transfer;


public:
    string getOperatorId() { return this->name; }






    FinalAggregationOperator(shared_ptr<DriverContext> driverContext,AggregationDesc desc) {


        this->desc = desc;
        this->finished = false;

        this->transfer = std::make_shared<DataPageTransfer>();
        this->driverContext = driverContext;

        vector<AggregateDesc> aggregateDesc = this->desc.getAggregates();

        for(int i = 0 ; i < aggregateDesc.size() ; i++)
        {
            arrow::acero::aggregate::Aggregate agg;
            if(aggregateDesc[i].getInputKey() == "")
                agg = arrow::acero::aggregate::Aggregate(aggregateDesc[i].getFunctionName(),aggregateDesc[i].getOutputName());
            else
                agg = arrow::acero::aggregate::Aggregate(aggregateDesc[i].getFunctionName(),ArrowFunctionOptionsSupplier::getOptions(aggregateDesc[i].getFunctionName()),
                                                         aggregateDesc[i].getInputKey(),aggregateDesc[i].getOutputName());

            aggregates.push_back(agg);
        }
        for(int i = 0 ; i < desc.getGroupByKeys().size() ; i++)
        {
            this->groupByKeys.push_back(arrow::FieldRef(desc.getGroupByKeys()[i]));
        }







    }
    FinalAggregationOperator() {

        this->finished = false;
    }
    void addInput(std::shared_ptr<DataPage> input) override {
        if (input != NULL) {
            this->inputPage = input;

            if (this->input_schema == NULL) {
                if(this->inputPage->isEndPage())
                {
                    this->outputResultCompeleted = true;
                    return;
                }
                else {

                    this->input_schema = this->inputPage->get()->schema();
                    thread(GenerateAgg, this).detach();
                }
            }

            bool ok;
            do{
               ok = this->transfer->givePage(this->inputPage);
            }
            while (!ok);

        }

    }

    static void GenerateAgg(FinalAggregationOperator *finalAgg)
    {





        auto source_node_options = arrow::acero::SourceNodeOptions{finalAgg->input_schema,finalAgg->MakeGenerator(finalAgg->transfer)};

        arrow::acero::Declaration source{"source", std::move(source_node_options)};

        auto aggregate_options =
                arrow::acero::AggregateNodeOptions{/*aggregates=*/finalAgg->aggregates,
                        /*keys=*/finalAgg->groupByKeys};
        arrow::acero::Declaration aggregate{
                "aggregate", {std::move(source)}, std::move(aggregate_options)};

        arrow::Result<shared_ptr<arrow::Table>> re =  arrow::acero::DeclarationToTable(std::move(aggregate));
        if(!re.status().ok())
        {
            spdlog::critical(re.status().ToString());
        }

        std::shared_ptr<arrow::Table> response_table = re.ValueOrDie();

        finalAgg->resultLock.lock();
        finalAgg->aggResult = response_table;
        finalAgg->reader = std::make_shared<arrow::TableBatchReader>(finalAgg->aggResult);
        finalAgg->resultLock.unlock();

    }





    arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> MakeGenerator(std::shared_ptr<DataPageTransfer> dataPageTransfer) {

        class State {
        public:
            std::shared_ptr<DataPageTransfer> dataPageTransfer;
            State(std::shared_ptr<DataPageTransfer> trans) { dataPageTransfer = trans;}

        };

        auto state = std::make_shared<State>(dataPageTransfer);

        return [state]() {

            std::shared_ptr<DataPage> page = state->dataPageTransfer->getPage();

            if(page->isEndPage())
                return arrow::AsyncGeneratorEnd<std::optional<arrow::ExecBatch>>();
            else
                return arrow::Future<std::optional<arrow::ExecBatch>>::MakeFinished(arrow::ExecBatch(*(page->get())));
        };

    }



    bool produceOutput()
    {
        this->resultLock.lock();

        if(this->aggResult == NULL || this->reader == NULL)
        {
            this->resultLock.unlock();
            return false;
        }

        this->resultLock.unlock();

        if(this->aggResult != NULL && this->reader != NULL)
        {
            std::shared_ptr<arrow::RecordBatch> batch = NULL;
            arrow::Status status = this->reader->ReadNext(&batch);
            if(status.ok()) {
                if(batch == NULL) {
                    this->inputPage = NULL;
                    this->reader = NULL;
                    this->aggResult = NULL;
                    this->outPutPage = NULL;
                    this->outputResultCompeleted = true;

                }
                else
                {
                    this->outPutPage = std::make_shared<DataPage>(batch);

                }
            }
            else
            {
                spdlog::critical("Final Agg Batch producing ERROR!"+status.ToString());
            }


        }
        return true;
    }


    std::shared_ptr<DataPage> getOutput() override {


        if(this->sendEndPage)
        {
            if(this->outputResultCompeleted) {
                this->finished = true;
                return DataPage::getEndPage();
            }
            else
            {
                this->produceOutput();
                return this->outPutPage;
            }

        }

        if(this->inputPage == NULL)
            return NULL;

        if(this->inputPage->isEndPage()) {
            this->sendEndPage = true;
            produceOutput();
            return this->outPutPage;
        }
        else
            this->inputPage = NULL;





        return NULL;

    }


    bool needsInput() override {
       return this->inputPage == NULL && this->outPutPage == NULL;
    }


    bool isFinished() {
        return this->finished;
    }


};




#endif //OLVP_FINALAGGREGATIONOPERATOR_HPP
