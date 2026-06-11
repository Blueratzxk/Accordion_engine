//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_LOOKUPJOINOPERATOR_HPP
#define OLVP_LOOKUPJOINOPERATOR_HPP

#include "../Operators/Operator.hpp"

#include "Join/PartitionedLookUpSourceFactory.hpp"
#include "Join/JoinProbe.hpp"
#include "Join/LookupJoinPageBuilder.hpp"
#include <arrow/io/api.h>
#include "arrow/csv/api.h"
#include "arrow/csv/writer.h"
#include <arrow/io/file.h>
#include <arrow/table.h>
#include "arrow/ipc/writer.h"
#include "../Utils/TimeCommon.hpp"

class LookupJoinOperator :public Operator{


    bool finished;
    string operatorId;
    string name = "LookupJoinOperator";


    bool sendEndPage = false;

    bool aborted = false;



    std::shared_ptr<LookupSourceProvider> lookupSourceProvider = NULL;
    std::shared_ptr<LookupSourceFactory> lookupSourceFactory = NULL;
    atomic<bool> lookupsourceStatus = false;

    std::future<std::shared_ptr<LookupSourceProvider>> lookupSourceProviderFuture;

    std::shared_ptr <DataPage> inputPage = NULL;
    std::shared_ptr <DataPage> outPutPage = NULL;


    std::shared_ptr<JoinProbeFactory> joinProbeFactory;
    std::shared_ptr<JoinProbe> probe = NULL;
    int64_t joinPosition = - 1;
    std::shared_ptr<LookupJoinPageBuilder> pageBuilder = NULL;



    std::shared_ptr <arrow::Schema> probeSchema = NULL;
    std::shared_ptr <arrow::Schema> buildSchema = NULL;

    bool currentProbePositionProducedRow = true;

    shared_ptr<DriverContext> driverContext;

    string buildSideRemoteSourceOperatorId;
    string buildOperatorId;

    long inputPageCounter = 0;
    int tupleCounter = 0;

    bool operatorMigration = false;

public:




    LookupJoinOperator(string operatorId,shared_ptr<DriverContext> driverContext,std::shared_ptr <arrow::Schema> probeSchema,
                       std::shared_ptr <arrow::Schema> buildOutputSchema,std::shared_ptr<JoinProbeFactory> joinProbeFactory,
                       std::shared_ptr<LookupSourceFactory> lookupSourceFactory,string buildSideRemoteSourceOperatorId,string buildOperatorId):Operator("LookupJoinOperator") {

        this->operatorId = operatorId;
        this->finished = false;
        this->joinProbeFactory = joinProbeFactory;
        this->lookupSourceFactory = lookupSourceFactory;

        this->probeSchema = probeSchema;
        this->buildSchema = buildOutputSchema;
        this->lookupSourceProviderFuture = this->lookupSourceFactory->createLookUpSourceProvider();
        this->pageBuilder = std::make_shared<LookupJoinPageBuilder>(buildOutputSchema);
        this->driverContext = driverContext;

        this->buildSideRemoteSourceOperatorId = buildSideRemoteSourceOperatorId;
        this->buildOperatorId = buildOperatorId;
    }




    void addInput(std::shared_ptr <DataPage> input) override {
        if (input != NULL && !input->isEndPage()) {
            this->inputPage = input;
            if (this->probeSchema == NULL) {
                this->probeSchema = this->inputPage->get()->schema();
            }


            this->joinPosition = -1;
            this->probe = this->joinProbeFactory->createJoinProbe(this->inputPage);
            inputPageCounter++;

        } else {

            this->inputPage = input;
        }

    }

    shared_ptr<arrow::Table> getLookupSourceData()
    {
        std::shared_ptr<PartitionedLookupSourceFactory::DefaultLookupSourceProvider> provider = static_pointer_cast<PartitionedLookupSourceFactory::DefaultLookupSourceProvider>(this->lookupSourceProvider);
        return provider->getSourceData();
    }

    shared_ptr<OperatorResponse> getJoinBuildComponents()
    {
        std::shared_ptr<PartitionedLookupSourceFactory::DefaultLookupSourceProvider> provider = static_pointer_cast<PartitionedLookupSourceFactory::DefaultLookupSourceProvider>(this->lookupSourceProvider);


        spdlog::info("LookupJoinOperator !#@!#@!@@!#!##@!!@##@!#@!#!#@!111111");
        auto buildComponents = provider->getBuildComponents();
        vector<shared_ptr<DataPage>> pagesToUpload;



        int tupleCount = 0;

        for(auto com : buildComponents)
        {
            auto pages = com->ToDataPages();
            for(auto page : pages) {
                pagesToUpload.push_back(page);
                tupleCount += page->getElementsCount();
            }

        }
        spdlog::info("LookupJoinOperator !#@!#@!@@!#!##@!!@##@!#@!#!#@!222222");
        long totalDataSize = 0;
        for (auto page : pagesToUpload) {

            auto fields = page->get()->schema()->fields();
            long tupleSize = 0;
            for (auto field : fields)
                tupleSize += field->type()->byte_width();
            totalDataSize += (tupleSize * page->getElementsCount());

        }

        this->driverContext->savePagesForInterTaskMission(this->buildOperatorId,pagesToUpload);
        this->driverContext->savePagesForInterTaskMission(this->buildOperatorId,{DataPage::getEndPage()});




        this->driverContext->reportExternalUploadTuples(this->operatorId, tupleCount);

        spdlog::info("LookupJoinOperator !#@!#@!@@!#!##@!!@##@!#@!#!#@!33333");
        shared_ptr<OperatorResponse> response = make_shared<OperatorResponse>();
        response->addOperatorId_Parameters(this->buildOperatorId,{"HASH_DATA","TABLE_DATA"});
        response->addOperatorId(this->buildOperatorId,OperatorResponse::MIGRATION);
        response->addOperatorId(this->buildSideRemoteSourceOperatorId,OperatorResponse::CLOSE);
        response->setMigrationDataSize(totalDataSize);
        return response;
    }


    shared_ptr<OperatorResponse> externalEvent(string parameters) override {

        if (!this->lookupsourceStatus) {
            this->operatorMigration = true;
            shared_ptr<OperatorResponse> response = make_shared<OperatorResponse>();
            response->addOperatorId(this->buildSideRemoteSourceOperatorId,OperatorResponse::NOT_BUILD_COMPLETE);

            return response;

        }

        if (parameters == "DIRECT_HASHTABLE_INSTALL") {
            return this->getJoinBuildComponents();
        }

        auto table = getLookupSourceData();
        if (table == NULL) {
            this->driverContext->savePagesForInterTaskMission(this->buildSideRemoteSourceOperatorId,
                                                              {DataPage::getEndPage()});

            this->driverContext->reportExternalUploadTuples(this->operatorId, -1);

            shared_ptr<OperatorResponse> response = make_shared<OperatorResponse>();
            response->addOperatorId(this->buildSideRemoteSourceOperatorId,OperatorResponse::MIGRATION);

            return response;
        }

        auto batches = table->CombineChunksToBatch();
        auto uploadedPage = make_shared<DataPage>(batches.ValueOrDie());
        this->driverContext->savePagesForInterTaskMission(this->buildSideRemoteSourceOperatorId,
                                                          {uploadedPage});
        this->driverContext->savePagesForInterTaskMission(this->buildSideRemoteSourceOperatorId,
                                                          {DataPage::getEndPage()});

        int tupleCount = 0;
        tupleCount += uploadedPage->getElementsCount();
        this->driverContext->reportExternalUploadTuples(this->operatorId, tupleCount);

        shared_ptr<OperatorResponse> response = make_shared<OperatorResponse>();
        response->addOperatorId(this->buildSideRemoteSourceOperatorId,OperatorResponse::MIGRATION);

        return response;
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
            lookupSourceProvider = this->lookupSourceProviderFuture.get();
            lookupsourceStatus = true;
            if(this->operatorMigration)
            {
                auto table = getLookupSourceData();
                if(table == NULL) {
                    this->driverContext->savePagesForInterTaskMission(this->buildSideRemoteSourceOperatorId,
                                                                      {DataPage::getEndPage()});
                    this->driverContext->reportExternalUploadTuples(this->operatorId,-1);
                }
                else {
                    auto batches = table->CombineChunksToBatch();
                    auto uploadedPage = make_shared<DataPage>(batches.ValueOrDie());
                    this->driverContext->savePagesForInterTaskMission(this->buildSideRemoteSourceOperatorId,{uploadedPage});
                    this->driverContext->savePagesForInterTaskMission(this->buildSideRemoteSourceOperatorId,{DataPage::getEndPage()});


                    this->driverContext->reportExternalUploadTuples(this->operatorId,uploadedPage->getElementsCount());
                }
            }
            return true;

            return false;
        }

        return true;
    }

    void processProbe()
    {
        std::shared_ptr<PartitionedLookupSourceFactory::DefaultLookupSourceProvider> provider = static_pointer_cast<PartitionedLookupSourceFactory::DefaultLookupSourceProvider>(this->lookupSourceProvider);

        provider->withLease<LookupJoinOperator*,bool>(this,
                                                      [](LookupJoinOperator* lookupJoinOperator,std::shared_ptr<DefaultLookupSourceLease> lease){
                                                                lookupJoinOperator->processProbe(lease->getLookupSource());
                                                                return true;
                                                        });

    }


    void buildPage()
    {

        if (pageBuilder->isEmpty()) {
            return;
        }

        this->outPutPage = pageBuilder->build(probe);
        this->pageBuilder->ResetStatus();
    }
    bool tryBuildPage()
    {
        if (pageBuilder->isFull()) {
        //    cout << "full!"<<endl;
            buildPage();
            return true;
        }
        return false;
    }

    bool joinCurrentPosition(std::shared_ptr<LookupSource> lookupSource)
    {
        // while we have a position on lookup side to join against...
        while (this->joinPosition >= 0) {
            if (lookupSource->isJoinPositionEligible(joinPosition, this->probe->getProbePosition(), probe->getPage())) {
                this->currentProbePositionProducedRow = true;

                this->pageBuilder->appendRow(probe, lookupSource, joinPosition);

            }

            // get next position on lookup side for this probe row
            joinPosition = lookupSource->getNextJoinPosition(joinPosition, this->probe->getProbePosition(), this->probe->getPage());

            if (tryBuildPage()) {
                return false;
            }
        }
        return true;
    }

    void clearProbe()
    {
        // Before updating the probe flush the current page
        buildPage();
        this->probe = NULL;
    }

    bool advanceProbePosition(std::shared_ptr<LookupSource> lookupSource)
    {
        if (!this->probe->advanceNextProbePostion()) {
            clearProbe();
            return false;
        }

        // update join position
        this->joinPosition = probe->getCurrentJoinPosition(lookupSource);
        return true;
    }

    void processProbe(std::shared_ptr<LookupSource> lookupSource) {

        while (true) {

            if (this->probe->getProbePosition() >= 0) {
                if (!joinCurrentPosition(lookupSource)) {
                    break;
                }
            }
            if (!advanceProbePosition(lookupSource)) {
                break;
            }
        }
    }


    std::shared_ptr<DataPage> process() {



        if (this->inputPage->getElementsCount() == 0) {
            this->inputPage = NULL;
            this->probe = NULL;
            this->outPutPage = NULL;
            return NULL;
        }


        if(this->probe == NULL && this->pageBuilder->isEmpty())
        {
            return NULL;
        }

        //if(!tryFetchLookupSourceProvider())
        //    return NULL;


        if(!this->lookupSourceProvider->isLookupSourceExist())
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


    void waitingBuildSideAndCheckAborted()
    {
        tryFetchLookupSourceProvider();
        if(this->lookupSourceFactory->isAborted())
            this->aborted = true;
    }

    std::shared_ptr <DataPage> getOutput() override {


        if (this->sendEndPage) {
            this->finished = true;

            spdlog::info("lookupjoin process "+ to_string(this->inputPageCounter)+ " pages");
            return DataPage::getEndPage();
           }


        waitingBuildSideAndCheckAborted();

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

    bool isAborted() override
    {
        return this->aborted;
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
        this->lookupSourceFactory->cancelLookupSource();
    }

};



#endif //OLVP_LOOKUPJOINOPERATOR_HPP
