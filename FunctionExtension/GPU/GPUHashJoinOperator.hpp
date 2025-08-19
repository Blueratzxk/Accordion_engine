//
// Created by zxk on 3/4/25.
//

#ifndef OLVP_GPUHASHJOINOPERATOR_HPP
#define OLVP_GPUHASHJOINOPERATOR_HPP


#include "../Operators/Operator.hpp"
#include "../../Execution/Task/Context/DriverContext.h"
#include "arrow/api.h"
#include "GPUFunctions.h"

class GPUHashJoinOperator :public Operator {


    bool finished;

    string name = "GPUHashJoinOperator";


    bool sendEndPage = false;


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;

    std::shared_ptr<DataPage> probe = NULL;

    shared_ptr<arrow::Schema> probeInputSchema;
    shared_ptr<arrow::Schema> buildInputSchema;
    shared_ptr<arrow::Schema> buildOutputSchema;
    vector<int> probeHashChannels;
    vector<int> buildHashChannels;
    vector<int> probeOutputChannels;
    vector<int> buildOutputChannels;

    shared_ptr<arrow::Schema> outputSchema;



    shared_ptr<DriverContext> driverContext;

    GPUFunctions gpuFunctions;

    shared_ptr<arrow::Table> buildTable = NULL;
    std::shared_ptr<LookupSourceFactory> lookupSourceFactory = NULL;
    std::shared_ptr<LookupSourceProvider> lookupSourceProvider = NULL;
    std::future<std::shared_ptr<LookupSourceProvider>> future;

    string buildTableToken;

public:


    string getOperatorId() { return this->name; }

    GPUHashJoinOperator(shared_ptr<DriverContext> driverContext, shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,
                        shared_ptr<arrow::Schema> buildOutputSchema,vector<int> probeHashChannels,vector<int> buildHashChannels,
                        vector<int> probeOutputChannels,vector<int> buildOutputChannels,shared_ptr<LookupSourceFactory> lookupSourceFactory) {

        this->finished = false;
        this->driverContext = driverContext;
        this->probeInputSchema = probeInputSchema;
        this->buildInputSchema = buildInputSchema;
        this->buildOutputSchema = buildOutputSchema;
        this->probeHashChannels = probeHashChannels;
        this->buildHashChannels = buildHashChannels;
        this->probeOutputChannels = probeOutputChannels;
        this->buildOutputChannels = buildOutputChannels;


        this->lookupSourceFactory = lookupSourceFactory;

        future = this->lookupSourceFactory->createLookUpSourceProvider();

        vector<shared_ptr<arrow::Field>> fields;
        for(auto outputChannel : probeOutputChannels)
            fields.push_back(this->probeInputSchema->field(outputChannel));

        for(auto field : this->buildOutputSchema->fields())
            fields.push_back(field);
        this->outputSchema = make_shared<arrow::Schema>(fields);




    }

    void addInput(std::shared_ptr<DataPage> input) override {
        if (input != NULL && !input->isEndPage()) {
            this->inputPage = input;
            if (this->probeInputSchema == NULL) {
                this->probeInputSchema = this->inputPage->get()->schema();
            }
            this->probe = this->inputPage;
        } else {
            this->inputPage = input;
        }

    }

    shared_ptr<arrow::Table> getLookupSourceData()
    {
        std::shared_ptr<PartitionedLookupSourceFactory::DefaultLookupSourceProvider> provider = static_pointer_cast<PartitionedLookupSourceFactory::DefaultLookupSourceProvider>(this->lookupSourceProvider);
        return provider->getSourceData();
    }

    bool tryFetchLookupSourceProvider()
    {
        if (this->lookupSourceProvider == NULL) {
            /*
            std::future_status status = this->lookupSourceProviderFuture.wait_for(std::chrono::seconds(0));
            if(status == std::future_status::ready) {
                lookupSourceProvider = lookupSourceProviderFuture.get();
                return true;
            }
             */

            this->lookupSourceFactory->tryGetCompletedLookupSource();
            lookupSourceProvider = this->future.get();
            this->buildTable = getLookupSourceData();
            GPUFunctions gpuFunctions;
            this->buildTableToken = gpuFunctions.getNextTokenStr();
            gpuFunctions.maintainBuildTableByToken(buildTableToken,this->buildTable);

            return true;
        }

        return true;
    }

    void processProbe()
    {
        void *outputTable;
        int elementCount;
        gpuFunctions.inner_join_byToken(probeInputSchema,buildInputSchema,buildOutputSchema,probeHashChannels,
                                        buildHashChannels,probeOutputChannels,buildOutputChannels,
                                        inputPage->getExtensionPage(),this->buildTableToken,outputTable,elementCount);
        this->outPutPage = make_shared<DataPage>(outputTable,outputSchema,elementCount,DataPage::GPU);
        this->probe = NULL;
    }

    std::shared_ptr<DataPage> process() {


        if (this->inputPage->getElementsCount() == 0) {
            this->inputPage = NULL;
            this->probe = NULL;
            this->outPutPage = NULL;
            return NULL;
        }


        if(this->probe == NULL && this->buildTable == NULL)
        {
            return NULL;
        }

        if(!tryFetchLookupSourceProvider())
            return NULL;


       // if(!this->lookupSourceProvider->isLookupSourceExist()) {
       //     this->probe = NULL;
       // }
       if(this->buildTable->num_rows() == 0)
           this->probe = NULL;

        if(finished)
            return NULL;


        if(this->probe != NULL)
        {
            this->processProbe();
        }

        if(this->outPutPage != NULL)
        {
            std::shared_ptr<DataPage> output = this->outPutPage;
            this->outPutPage = NULL;
            return output;
        }

        return NULL;



    }


    std::shared_ptr<DataPage> getOutput() override {



        if (this->sendEndPage) {
            this->finished = true;
            gpuFunctions.deleteBuildTableByToken(this->buildTableToken);
            return DataPage::getEndPage();
        }



        if(this->inputPage != NULL) {


            if (this->inputPage->isEndPage()) {
                this->sendEndPage = true;
                return this->outPutPage;
            }

            return this->process();

        }

        return NULL;


    }


    bool needsInput() override {
        return probe == NULL && outPutPage == NULL;
    }


    bool isFinished() {
        return this->finished;
    }

    int isExtension() override
    {
        return true;
    }
    shared_ptr<DataPage> downloadToCPU(shared_ptr<DataPage> page) override
    {
        if(page->isEndPage())
            return page;
        auto table = gpuFunctions.getCPUPageFromGPU(page->getExtensionPage(),outputSchema);
        return make_shared<DataPage>(table->CombineChunksToBatch().ValueOrDie());
    }
    shared_ptr<DataPage> uploadToExtension(shared_ptr<DataPage> page) override
    {
        spdlog::info("hashjoin uploadToExtension:"+ to_string(page->getElementsCount()));
        if(page->isEndPage())
            return page;
        return make_shared<DataPage>(gpuFunctions.pushCPUPageToGPU(page->get()),page->get()->schema(),page->getElementsCount(),DataPage::GPU);
    }
};



#endif //OLVP_GPUHASHJOINOPERATOR_HPP
