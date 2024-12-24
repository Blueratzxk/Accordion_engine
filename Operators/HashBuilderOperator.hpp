//
// Created by zxk on 5/25/23.
//

#ifndef OLVP_HASHBUILDEROPERATOR_HPP
#define OLVP_HASHBUILDEROPERATOR_HPP


#include "../Operators/Operator.hpp"

#include "../Page/Channel.hpp"
#include "../Utils/ArrowArrayBuilder.hpp"
#include "Join/HashTableLookupSource/PagesIndex.hpp"
#include "Join/PartitionedLookUpSourceFactory.hpp"
class HashBuilderOperator:public Operator
{

public:
    enum State{
        CONSUMING_INPUT,
        SPILLING_INPUT,
        LOOKUP_SOURCE_BUILT,
        INPUT_SPILLED,
        INPUT_UNSPILLING,
        INPUT_UNSPILLED_AND_BUILT,
        CLOSED
    };

private:
    bool finished = false;


    string name = "HashBuilderOperator";

    bool sendEndPage = false;


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;


    std::shared_ptr<arrow::Schema> inputSchema = NULL;

    vector<int> outputChannels;
    vector<int> hashChannels;
    std::shared_ptr<PagesIndex> pagesIndex;

    State state= State::CONSUMING_INPUT;

    std::shared_ptr<LookupSourceSupplier> lookupSourceSupplier;

    int partitionIndex;
    int pageCounter = 0;
    int tupleCounter = 0;

    std::shared_ptr<PartitionedLookupSourceFactory> lookupSourceFactory;

    shared_ptr<DriverContext> driverContext;

    shared_ptr<std::chrono::system_clock::time_point> firstStartBuildTime = NULL;
    shared_ptr<std::chrono::system_clock::time_point> lastBuildFinishedTime = NULL;
    shared_ptr<std::chrono::system_clock::time_point> buildComputingStartTime = NULL;

    string joinId;
public:



    string getOperatorId() { return this->name; }

    HashBuilderOperator(string joinId,shared_ptr<DriverContext> driverContext,int partitionIndex,std::shared_ptr<PartitionedLookupSourceFactory> lookupSourceFactory,
                        vector<int> outputChannels,vector<int> hashChannels) {

        this->finished = false;
        this->partitionIndex = partitionIndex;
        this->lookupSourceFactory = lookupSourceFactory;
        this->inputSchema = this->lookupSourceFactory->getBuildInputSchema();
        this->pagesIndex = std::make_shared<PagesIndex>(this->inputSchema);
        this->outputChannels = outputChannels;
        this->hashChannels = hashChannels;
        this->driverContext = driverContext;
        this->joinId = joinId;

    }
    State getState()
    {
        return this->state;
    }


    void updateIndex(std::shared_ptr<DataPage>  page)
    {
        this->pagesIndex->addPage(page);
    }


    void addInput(std::shared_ptr<DataPage> input) override {
        if(input != NULL && !input->isEndPage()) {

           // if(this->state == STANDBY)
           // {
          //      this->driverContext->reportBuildStartTime(std::chrono::system_clock::now());
          //  }

          if(!input->isEmptyPage()) {
              if (this->firstStartBuildTime == NULL)
                  this->firstStartBuildTime = make_shared<std::chrono::system_clock::time_point>(
                          std::chrono::system_clock::now());
          }

         //   this->state = CONSUMING_INPUT;
            this->inputPage = input;

            updateIndex(this->inputPage);

            pageCounter++;
            this->tupleCounter+=this->inputPage->getElementsCount();
        }
        else
        {
            if(!this->finished) {
                this->finishBuild();
                spdlog::debug("JoinHash Build Finished!");
                spdlog::info("HashBuilder operator processes "+ to_string(this->pageCounter) + " pages, "+ to_string(this->tupleCounter)+" tuples!");
            }
            this->finished = true;
        }

    }


    std::shared_ptr<LookupSourceSupplier> buildLookupSource()
    {
        this->driverContext->getBuildAllCount() += pagesIndex->getPositionCount();

        shared_ptr<LookupSourceSupplier> partition = pagesIndex->createLookupSourceSupplier(this->hashChannels,this->outputChannels,this->driverContext->getBuildProgress());
        this->lookupSourceSupplier = partition;
        return partition;
    }

    void finishInput()
    {
        std::shared_ptr<LookupSourceSupplier> partition = buildLookupSource();
        this->lookupSourceFactory->lendPartitionLookupSource(partitionIndex, partition);

        state = State::LOOKUP_SOURCE_BUILT;
    }

    void finishBuild()
    {

        switch (state) {
            case CONSUMING_INPUT:

                this->buildComputingStartTime = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());
                finishInput();

                driverContext->reportBuildComplete();
                this->lastBuildFinishedTime = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());

                double buildTime;
                if(this->lastBuildFinishedTime == NULL || this->firstStartBuildTime == NULL)
                    buildTime = 0;
                else
                    buildTime = std::chrono::duration<double, std::milli>(*this->lastBuildFinishedTime - *this->firstStartBuildTime).count();


                double buildComputingTime;
                if(this->lastBuildFinishedTime == NULL || this->buildComputingStartTime == NULL)
                    buildComputingTime = 0;
                else
                    buildComputingTime = std::chrono::duration<double, std::milli>(*this->lastBuildFinishedTime - *this->buildComputingStartTime).count();



                this->driverContext->reportBuildTime(buildTime);
                this->driverContext->reportBuildComputingTime(joinId,buildComputingTime);

                return;
        }
    }



    std::shared_ptr<DataPage> getOutput() override {

        return NULL;

    }


    bool needsInput() override {
        bool statesNeedInput = (state == State::CONSUMING_INPUT);
        return statesNeedInput;
    }


    bool isFinished()
    {
        return this->finished;
    }


};




#endif //OLVP_HASHBUILDEROPERATOR_HPP
