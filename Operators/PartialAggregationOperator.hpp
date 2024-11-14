//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_PARTIALAGGREGATIONOPERATOR_HPP
#define OLVP_PARTIALAGGREGATIONOPERATOR_HPP


#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"

//#include "arrow/acero/groupby.h"
#include "arrow/compute/exec.h"
#include "arrow/acero/aggregate_node.h"
#include "arrow/table.h"

#include "../Operators/Operator.hpp"
#include "../Descriptor/AggregationDescriptor.hpp"
#include "../Utils/ArrowFunctionOptionsSupplier.hpp"

class PartialAggregationOperator:public Operator {

    bool finished;

    string name = "PartialAggregationOperator";


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;


    std::shared_ptr<arrow::Schema> input_schema;

    std::shared_ptr<arrow::Schema> output_schema;

    AggregationDesc desc;


    vector<arrow::acero::aggregate::Aggregate> aggregates;
    vector<arrow::FieldRef> groupByKeys;

    shared_ptr<arrow::Table> aggResult = NULL;
    shared_ptr<arrow::TableBatchReader> reader = NULL;

    bool outputCompleted = true;
    bool sendEndPage = false;

    int count = 0;

    shared_ptr<DriverContext> driverContext;
public:
    string getOperatorId() { return this->name; }

    PartialAggregationOperator(shared_ptr<DriverContext> driverContext,AggregationDesc desc){


        this->desc = desc;
        this->finished = false;
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



    void addInput(std::shared_ptr<DataPage> input) override {
        if (input != NULL) {
            this->inputPage = input;
            count++;

            if (this->inputPage->isEndPage()) {

                this->sendEndPage = true;
            }

        }

    }

    arrow::Result<std::shared_ptr<arrow::Table>> TableGroupBy(std::shared_ptr<arrow::Table> table,
                                                std::vector<arrow::Aggregate> aggregates,
                                                std::vector<arrow::FieldRef> keys, bool use_threads = false) {
        arrow::acero::Declaration plan = arrow::acero::Declaration::Sequence(
                {{"table_source", arrow::acero::TableSourceNodeOptions(table)},
                 {"aggregate", arrow::acero::AggregateNodeOptions(aggregates, keys)}});
        return DeclarationToTable(std::move(plan), use_threads);
    }

    void process() {


        std::shared_ptr<arrow::compute::CountOptions> count_options = std::make_shared<arrow::compute::CountOptions>() ;
        std::shared_ptr<arrow::compute::ScalarAggregateOptions> scalar_aggregate_options = std::make_shared<arrow::compute::ScalarAggregateOptions>() ;



       // arrow::Table::Make(this->inputPage->get()->schema(),this->inputPage->get()->columns());

        shared_ptr<arrow::Table> table = arrow::Table::FromRecordBatches({this->inputPage->get()}).ValueOrDie();


        arrow::Result<shared_ptr<arrow::Table>> result =  this->TableGroupBy(table,this->aggregates,this->groupByKeys);





       // arrow::Result<std::shared_ptr<arrow::Table>> result = arrow::acero::BatchGroupBy(this->inputPage->get(),this->aggregates,this->groupByKeys,false);

        if(!result.ok())
        {
            spdlog::critical("Aggregation Error!" + result.status().ToString());
            this->inputPage = NULL;
            return;
        }

        this->aggResult = result.ValueOrDie();
        this->reader = std::make_shared<arrow::TableBatchReader>(this->aggResult);


    }

    void produceOutput()
    {
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
                    this->outputCompleted = true;
                }
                else
                {
                    this->outPutPage = std::make_shared<DataPage>(batch);
                }
            }
            else
            {
                spdlog::critical("Partial Agg Batch producing ERROR!"+status.ToString());
            }


        }

    }


    std::shared_ptr<DataPage> getOutput() override {


        if(this->sendEndPage)
        {
            if(this->outputCompleted) {
                this->finished = true;
                spdlog::debug("PartialAgg process "+ to_string(this->count)+" pages.");
                return DataPage::getEndPage();
            }

        }


        if (this->inputPage == NULL)
            return NULL;








        if(this->outputCompleted == true)
        {
            if(!this->inputPage->isEndPage()) {
                process();
                this->outputCompleted = false;
            }
        }
        if(this->outputCompleted == false)
            produceOutput();



        return this->outPutPage;

    }


    bool needsInput() override {
        if (this->inputPage == NULL)
            return true;
        else
            return false;
    }


    bool isFinished() override {
        return this->finished;
    }


};




#endif //OLVP_PARTIALAGGREGATIONOPERATOR_HPP
