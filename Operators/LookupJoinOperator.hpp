//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_LOOKUPJOINOPERATOR_HPP
#define OLVP_LOOKUPJOINOPERATOR_HPP

#include "../Operators/Operator.hpp"
#include "Join/LookupSourceProvider.hpp"
#include "Join/PartitionedLookUpSourceFactory.hpp"
#include "Join/JoinProbe.hpp"
#include "Join/LookupJoinPageBuilder.hpp"



class LookupJoinOperator :public Operator{


    bool finished;

    string name = "LookupJoinOperator";


    bool sendEndPage = false;



    std::shared_ptr<LookupSourceProvider> lookupSourceProvider = NULL;
    std::shared_ptr<LookupSourceFactory> lookupSourceFactory = NULL;

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
public:


    string getOperatorId() { return this->name; }

    LookupJoinOperator(shared_ptr<DriverContext> driverContext,std::shared_ptr <arrow::Schema> probeSchema,std::shared_ptr <arrow::Schema> buildOutputSchema,std::shared_ptr<JoinProbeFactory> joinProbeFactory,std::shared_ptr<LookupSourceFactory> lookupSourceFactory) {

        this->finished = false;
        this->joinProbeFactory = joinProbeFactory;
        this->lookupSourceFactory = lookupSourceFactory;

        this->probeSchema = probeSchema;
        this->buildSchema = buildOutputSchema;
        this->lookupSourceProviderFuture = this->lookupSourceFactory->createLookUpSourceProvider();
        this->pageBuilder = std::make_shared<LookupJoinPageBuilder>(buildOutputSchema);
        this->driverContext = driverContext;
    }

    void addInput(std::shared_ptr <DataPage> input) override {
        if (input != NULL && !input->isEndPage()) {
            this->inputPage = input;
            if (this->probeSchema == NULL) {
                this->probeSchema = this->inputPage->get()->schema();
            }


            this->joinPosition = -1;
            this->probe = this->joinProbeFactory->createJoinProbe(this->inputPage);



        } else {

            this->inputPage = input;
        }

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

        if(!tryFetchLookupSourceProvider())
            return NULL;

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


    std::shared_ptr <DataPage> getOutput() override {


        if (this->sendEndPage) {
            this->finished = true;
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

};



#endif //OLVP_LOOKUPJOINOPERATOR_HPP
