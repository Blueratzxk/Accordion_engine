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
class GPUBatchAssembleOperator:public Operator {
public:


    bool finished;
    string name = "GPUBatchAssembleOperator";

    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;

    std::shared_ptr<arrow::Schema> input_schema = NULL;


    GPUFunctions gpuFunctions;

    shared_ptr<DriverContext> driverContext;

    bool sendEndPage = false;

    vector<shared_ptr<arrow::RecordBatch>> sink;

    bool sendBigChunk = false;
    shared_ptr<DataPage> bigPage = NULL;

    long totalElementCount = 0;


public:
    string getOperatorId() { return this->name; }


    GPUBatchAssembleOperator(shared_ptr<DriverContext> driverContext) {

        this->finished = false;
        this->driverContext = driverContext;

    }

    bool isTimeToSendBigPage()
    {
        if(this->totalElementCount >= 10000000)
            return true;
        else
            return false;
    }

    void reset()
    {
        this->sendBigChunk = false;
        this->totalElementCount = 0;
        this->sink.clear();
    }


    void makeGPUBigPageAndSend() {

        auto bigChunk = arrow::ConcatenateRecordBatches(sink).ValueOrDie();
        auto bigCPUPage = make_shared<DataPage>(bigChunk);

        this->bigPage = make_shared<DataPage>(gpuFunctions.pushCPUPageToGPU(bigCPUPage->get()),
                                             this->input_schema, bigCPUPage->getElementsCount(), DataPage::GPU);
        this->sendBigChunk = true;
    }


    void addInput(std::shared_ptr<DataPage> input) override {

        if (input != NULL) {
            if (this->input_schema == NULL)
                this->input_schema = input->get()->schema();

            this->inputPage = input;
            if (!input->isEndPage()) {
                this->sink.push_back(input->get());

                this->totalElementCount += input->getElementsCount();

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

        if (!sendBigChunk)
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


};


#endif //OLVP_GPUBATCHASSEMBLEOPERATOR_HPP
