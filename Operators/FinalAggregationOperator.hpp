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
#include "../Utils/ColumnCompute.hpp"
#include "tbb/concurrent_queue.h"
#include "../Utils/BlockQueue.hpp"
#include "../Execution/Event/SimpleEvent.hpp"
#include  "../Execution/Task/Statistics/CardEstimator/HyperLogLog.hpp"
class FinalAggregationOperator:public Operator {

    bool finished;
    string operatorId;
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


    long totalElementCount = 0;
    bool intermediateMode = false;
    std::map<string/*inputKey*/,std::pair<string,string>/*functionName,outputName*/> intermediateTransformMap;
    bool operatorMigration = false;
    atomic<bool> waitInterTaskSync = false;
    shared_ptr<Event> interTaskEvent;
    list<shared_ptr<DataPage>> interTaskPages;

    shared_ptr<HyperLogLog> hyper_log_log;

public:

    void estimateGroupCount(shared_ptr<DataPage> page) {

        if (page->isEndPage())
            return;
        if (this->groupByKeys.empty())
            return;

        auto input = page->get();

        auto schema = input->schema();

        std::vector<std::shared_ptr<arrow::Field>> new_fields;
        std::vector<std::shared_ptr<arrow::Array>> new_arrays;

        for (auto& g : groupByKeys)
        {
            for (int i = 0; i < schema->num_fields(); i++)
            {
                if (schema->field(i)->name() == *(g.name()))
                {
                    new_fields.push_back(schema->field(i));
                    new_arrays.push_back(input->column(i)); // ? zero-copy
                    break;
                }
            }
        }

        auto new_schema = arrow::schema(new_fields);

        auto new_batch = arrow::RecordBatch::Make(
            new_schema,
            input->num_rows(),
            new_arrays
        );

        auto newDataPage = make_shared<DataPage>(new_batch);
        this->hyper_log_log->update(newDataPage);
    }

    shared_ptr<OperatorResponse> externalEvent(string parameter) override
    {
        return migrateOperator();
    }

    shared_ptr<OperatorResponse> migrateOperator()
    {
        if(this->finished)
            return NULL;

        auto response = make_shared<OperatorResponse>();
        response->addOperatorId(this->operatorId,OperatorResponse::MIGRATION);


        long tupleSize = 0;
        if (this->input_schema != NULL) {
            for (auto field : this->input_schema->fields()) {
                tupleSize += field->type()->byte_width();
            }
        }
        int tupleCount = this->hyper_log_log->getEstimation();
        response->setMigrationDataSize(tupleCount * tupleSize);
        operatorMigration = true;
        return response;
    }

    string  transformIntermiediate(string functionName,string outputName,string inputKey)
    {
        string transformFuncName = functionName;
        if(functionName == "hash_mean") {
            this->intermediateTransformMap[inputKey] = {functionName,outputName};
            transformFuncName = "None";
            this->intermediateMode = true;
        }

        return transformFuncName;
    }

    arrow::acero::aggregate::Aggregate getAggregation(AggregateDesc aggregateDesc)
    {

        string functionName = aggregateDesc.getFunctionName();
        string outputName = aggregateDesc.getOutputName();
        string inputKey = aggregateDesc.getInputKey();

        functionName = transformIntermiediate(functionName,outputName,inputKey);

        if(inputKey == "") {
            return arrow::acero::aggregate::Aggregate(functionName, outputName);
        }
        else {
            return arrow::acero::aggregate::Aggregate(functionName,
                                                      ArrowFunctionOptionsSupplier::getOptions(functionName), inputKey,
                                                      outputName);
        }
    }

    void setupAggregations()
    {

        vector<AggregateDesc> aggregateDesc = this->desc.getAggregates();

        for(int i = 0 ; i < aggregateDesc.size() ; i++)
        {
            arrow::acero::aggregate::Aggregate agg;
            agg = getAggregation(aggregateDesc[i]);
            if(agg.function != "None")
                aggregates.push_back(agg);
        }
        for(int i = 0 ; i < desc.getGroupByKeys().size() ; i++)
        {
            this->groupByKeys.push_back(arrow::FieldRef(desc.getGroupByKeys()[i]));
        }
    }

    FinalAggregationOperator(string operatorId, shared_ptr<DriverContext> driverContext,AggregationDesc desc):Operator("FinalAggregationOperator"){


        this->operatorId = operatorId;
        this->desc = desc;
        this->finished = false;

        this->transfer = std::make_shared<DataPageTransfer>();
        this->driverContext = driverContext;

        this->interTaskEvent = make_shared<SimpleEvent>();

        this->hyper_log_log = make_shared<HyperLogLog>();

        setupAggregations();

    }

    void waitSidewayDataSync()
    {
        this->waitInterTaskSync = true;
    }

    void fulfillExternalEventWithPages(vector<shared_ptr<DataPage>> pages) override
    {
        spdlog::warn("fulfillExternalEventWithPages!");
        if(this->waitInterTaskSync) {
            for(auto page : pages)
            {
                if(!page->isEndPage())
                    this->interTaskPages.push_back(page);
            }

            this->waitInterTaskSync = false;
            this->interTaskEvent->notify();
        }

    }

    FinalAggregationOperator():Operator("FinalAggregationOperator"){

        this->finished = false;
    }
    void addInput(std::shared_ptr<DataPage> input) override {
        if (input != NULL) {
            this->inputPage = input;
            if(this->inputPage->getElementsCount() > 0)
                this->totalElementCount += this->inputPage->getElementsCount();

            if (this->input_schema == NULL) {
                if(this->inputPage->isEndPage())
                {
                    this->outputResultCompeleted = true;
                    if(this->operatorMigration)
                        this->driverContext->savePagesForInterTaskMission(this->operatorId,{DataPage::getEndPage()});
                    return;
                }


                this->input_schema = this->inputPage->get()->schema();
                thread(GenerateAgg, this).detach();
                this->estimateGroupCount(input);
            }

            if(this->inputPage->isEndPage()) {
                spdlog::warn("Agg get end page Operator migration is "+ to_string(this->operatorMigration));
                supplyInterTaskData();
            }
            bool ok;
            do{
                this->estimateGroupCount(input);
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

            if(page->isEndPage()) {
                spdlog::info("Final Agg accept End Page!");
                return arrow::AsyncGeneratorEnd<std::optional<arrow::ExecBatch>>();
            }
            else
                return arrow::Future<std::optional<arrow::ExecBatch>>::MakeFinished(arrow::ExecBatch(*(page->get())));
        };

    }



    shared_ptr<arrow::RecordBatch> applyFinalToIntermediateOutput(shared_ptr<arrow::RecordBatch> intermediateOutput)
    {

        shared_ptr<arrow::RecordBatch> output = intermediateOutput;
        bool success = false;


        for(auto inter : intermediateTransformMap)
        {
            string inputColumnName = inter.first;
            string functionName = inter.second.first;
            string outputName = inter.second.second;

            if(functionName == "hash_mean") {
                success = ColumnComputeUtils::ComputeInAndToRecordBatch(output, output, "divide",
                                                                        inputColumnName, "groupBy_count", outputName);
            }
            else
                spdlog::error("applyFinalToIntermediateOutput error,no function found!");
        }

        return output;
    }

    shared_ptr<arrow::RecordBatch> produceAllOutput()
    {
        std::shared_ptr<arrow::RecordBatch> batch = NULL;
        vector<std::shared_ptr<arrow::RecordBatch>> recordBatchs;
        do {
            arrow::Status status = this->reader->ReadNext(&batch);
            if(status.ok())
            {

                if(batch != NULL)
                    recordBatchs.push_back(batch);
            }
            else
            {
                spdlog::critical("Final Agg Batch producing ERROR!"+status.ToString());
            }
        }
        while (batch != NULL);

        if(recordBatchs.empty())
            return batch;

        auto result = arrow::Table::FromRecordBatches(recordBatchs);
        shared_ptr<arrow::RecordBatch> final = NULL;
        shared_ptr<arrow::RecordBatch> allIntermediateOutput = result.ValueOrDie()->CombineChunksToBatch().ValueOrDie();


        if(this->operatorMigration) {
            this->driverContext->savePagesForInterTaskMission(this->operatorId,{make_shared<DataPage>(allIntermediateOutput)});
            this->driverContext->savePagesForInterTaskMission(this->operatorId,{DataPage::getEndPage()});
        }
        else {
            spdlog::info(allIntermediateOutput->ToString());
            final = applyFinalToIntermediateOutput(allIntermediateOutput);
        }
        return final;
    }

    void supplyInterTaskData()
    {
        if(this->waitInterTaskSync) {
            spdlog::warn("Need waitTaskSync,listen! Operator migration is "+ to_string(this->operatorMigration));
            this->interTaskEvent->listen();
            spdlog::warn("Listen go! Operator migration is "+ to_string(this->operatorMigration));
        }

        spdlog::warn("Input interTask Pages! Operator migration is "+ to_string(this->operatorMigration));
        for(auto page : this->interTaskPages)
        {
            spdlog::info(page->get()->ToString());

            bool ok;
            do{
                ok = this->transfer->givePage(page);
            }
            while (!ok);
            this->totalElementCount+=page->getElementsCount();
        }
        spdlog::warn("Input interTask Pages OK! Operator migration is "+ to_string(this->operatorMigration));
        this->interTaskPages.clear();
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
            arrow::Status status;

            if(this->intermediateMode) {
                batch = produceAllOutput();
                status = arrow::Status::OK();
            }
            else {
                status = this->reader->ReadNext(&batch);
            }


            if(status.ok()) {
                if(batch == NULL) {
                    this->inputPage = NULL;
                    this->reader = NULL;
                    this->aggResult = NULL;
                    this->outPutPage = NULL;
                    this->outputResultCompeleted = true;

                    if(this->operatorMigration)
                        this->driverContext->savePagesForInterTaskMission(this->operatorId,{DataPage::getEndPage()});
                }
                else
                {
                    if(this->operatorMigration)
                        this->driverContext->savePagesForInterTaskMission(this->operatorId,{make_shared<DataPage>(batch)});
                    else
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

                spdlog::info("Final agg process "+ to_string(this->totalElementCount)+ " pages");
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

        //    if(this->waitInterTaskSync)
         //       this->interTaskEvent->listen();

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

    string getOperatorId() override
    {
        return this->operatorId;
    }

    void abort() override {
        this->finished = true;
        if(this->waitInterTaskSync) {
            this->interTaskEvent->notify();
            this->waitInterTaskSync = false;
        }
    }


};




#endif //OLVP_FINALAGGREGATIONOPERATOR_HPP
