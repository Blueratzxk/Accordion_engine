//
// Created by zxk on 8/21/25.
//

#ifndef OLVP_GPUBATCHASSEMBLEOPERATOR_HPP
#define OLVP_GPUBATCHASSEMBLEOPERATOR_HPP


#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"

#include "arrow/acero/options.h"
#include "arrow/acero/aggregate_node.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/table.h"

#include "../Operators/Operator.hpp"
#include "GPUFunctions.h"
#include "../../Execution/Task/Context/DriverContext.h"
#include "Config/GPUExecutionConfig.hpp"
class GPUBatchAssembleOperator:public Operator {
public:

    bool finished;
    string name = "GPUBatchAssembleOperator";
    string operatorId;

    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;

    std::shared_ptr<arrow::Schema> input_schema = NULL;


    GPUFunctions gpuFunctions;

    shared_ptr<DriverContext> driverContext;

    bool sendEndPage = false;

    vector<shared_ptr<arrow::RecordBatch>> sink;

    bool sendBigPage = false;
    shared_ptr<DataPage> bigPage = NULL;

    long totalElementCount = 0;

    long totalBytes = 0;

    long assembleThresholdByBytes = 0;

    GPUExecutionConfig config;

public:

    GPUBatchAssembleOperator(string operatorId,shared_ptr<DriverContext> driverContext):Operator("GPUBatchAssembleOperator") {

        this->finished = false;
        this->driverContext = driverContext;
        this->assembleThresholdByBytes = config.getGPUBatchAssembleThreshold();
        this->operatorId = operatorId;

    }

    int64_t GetRecordBatchSize(const std::shared_ptr<arrow::RecordBatch>& batch) {
        int64_t total_bytes = 0;
        for (int i = 0; i < batch->num_columns(); ++i) {
            const auto& array = batch->column(i);
            const auto& data  = array->data();
            for (const auto& buffer : data->buffers) {
                if (buffer) total_bytes += buffer->size();
            }
        }
        return total_bytes;
    }


    bool isTimeToSendBigPage()
    {
        if(this->totalBytes >= this->assembleThresholdByBytes)
            return true;
        else
            return false;
    }

    void reset()
    {
        this->sendBigPage = false;
        this->totalElementCount = 0;
        this->totalBytes = 0;
        this->sink.clear();
    }


    void makeGPUBigPageAndSend() {

        if(!this->sink.empty()) {

            auto bigChunk = arrow::ConcatenateRecordBatches(this->sink).ValueOrDie();
            auto bigCPUPage = make_shared<DataPage>(bigChunk);
            this->bigPage = make_shared<DataPage>(gpuFunctions.pushCPUPageToGPU(bigCPUPage->get()),
                                                  this->input_schema, bigCPUPage->getElementsCount(), DataPage::GPU);
            this->sendBigPage = true;
        }
        else
        {
            this->bigPage = NULL;
            this->sendBigPage = true;
        }
    }


    void addInput(std::shared_ptr<DataPage> input) override {

        if (input != NULL) {

            this->inputPage = input;
            if (!input->isEndPage()) {

                if (this->input_schema == NULL)
                    this->input_schema = input->get()->schema();

                this->sink.push_back(input->get());

                this->totalElementCount += input->getElementsCount();
                this->totalBytes += GetRecordBatchSize(input->get());

                if(isTimeToSendBigPage())
                    makeGPUBigPageAndSend();
            }
        }

    }

    std::shared_ptr<DataPage> getOutput() override {

        if (this->finished)
            return DataPage::getEndPage();

        if (this->inputPage != NULL && this->inputPage->isEndPage()) {
            this->finished = true;
            makeGPUBigPageAndSend();
        }

        if (!sendBigPage)
            return NULL;
        else {
            reset();
            return this->bigPage;
        }

    }


    bool needsInput() override {
        return true;
    }


    bool isFinished() {
        return this->finished;
    }


    int isExtension() override {
        return true;
    }

    shared_ptr<DataPage> downloadToCPU(shared_ptr<DataPage> page) override {
        if (page->isEndPage())
            return page;
        auto table = gpuFunctions.getCPUPageFromGPU(page->getExtensionPage(), input_schema);
        return make_shared<DataPage>(table->CombineChunksToBatch().ValueOrDie());
    }

    shared_ptr<DataPage> uploadToExtension(shared_ptr<DataPage> page) override {
        //spdlog::info("BatchAssembleOp uploadToExtension:" + to_string(page->getElementsCount()));
        return page;
    }

    string getOperatorId() override
    {
        return this->operatorId;
    }

};


#endif //OLVP_GPUBATCHASSEMBLEOPERATOR_HPP
