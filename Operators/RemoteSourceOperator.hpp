//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_REMOTESOURCEOPERATOR_HPP
#define OLVP_REMOTESOURCEOPERATOR_HPP


#include "../Operators/Operator.hpp"
#include "../Web/ArrowRPC/DataPageRPCBuffer.hpp"
#include "../Web/ArrowRPC/RPCClient.hpp"
#include "../Execution/Event/SimpleEvent.hpp"

//class DriverContext;
using namespace  std;
class RemoteSourceOperator:public Operator
{

    bool finished;
    string operatorId;
    std::shared_ptr<DataPage> inputPage;

    string name = "RemoteSourceOperator";


    shared_ptr<RPCClient> client;

    int endSignalCount = 0;
    atomic<int> concurrentCount = 0 ;


    atomic<bool> abortTransmission = false;
    bool firstOutputCall = true;



    mutex lock;


    shared_ptr<DriverContext> driverContexts;



    bool hasBuildTask = false;
    bool hasNotifyBuild = false;

    mutex timelock;

    int traffic = 0;
    int maxTraffic = 0;
    shared_ptr<std::chrono::system_clock::time_point> start = NULL;
    int bufferTuneCircle = 500; //ms


    atomic<bool> waitInterTaskSync = false;
    shared_ptr<Event> sidewayTaskEvent = make_shared<SimpleEvent>();
    list<std::shared_ptr<DataPage>> sidewayPages;

    bool upstreamFinished = false;


    shared_ptr<DataPage> endPage = DataPage::getEndPage();

public:


    RemoteSourceOperator(string operatorId,shared_ptr<DriverContext> driverContext):Operator("RemoteSourceOperator")  {

        this->finished = false;
        this->operatorId = operatorId;
        this->client = make_shared<RPCClient>();
        this->driverContexts = driverContext;
        this->hasBuildTask = this->driverContexts->hasBuildTask();
        this->client->addDriverContext(this->driverContexts);

    }

    void waitSidewayDataSync()
    {
        this->waitInterTaskSync = true;
    }

    void fulfillExternalEventWithPages(vector<std::shared_ptr<DataPage>> pages) override
    {
        int tupleCount = 0;
        for(auto page : pages)
            if(!page->isEndPage()) {
                this->sidewayPages.push_back(page);
                tupleCount += page->getElementsCount();
            }

        this->driverContexts->reportExternalFulfillTuples(this->operatorId,tupleCount);
        this->waitInterTaskSync = false;
        this->sidewayTaskEvent->notify();
    }


    void addSources(set<shared_ptr<Split>> Splits)
    {
        if(abortTransmission)
            return;

        set<shared_ptr<Split>> regSplits;

        //if(this->concurrentCount < Splits.size())
          //  this->concurrentCount = Splits.size();
        for(auto split : Splits)
        {
            if(split->getConnectorSplit()->getId() == "RemoteSplit") {
                this->client->addLocation(static_pointer_cast<RemoteSplit>(split->getConnectorSplit()));
                regSplits.insert(split);
            }
            else if(split->getConnectorSplit()->getId() == "InterTaskSplit")
                this->client->addLocation(static_pointer_cast<InterTaskSplit>(split->getConnectorSplit()));
            else if(split->getConnectorSplit()->getId() == "NULLSplit")
                this->client->addLocation(static_pointer_cast<NULLSplit>(split->getConnectorSplit()));
            else
                spdlog::critical("RemoteSourceOperator: Unknown spilt type "+split->getConnectorSplit()->getId()+"!");

        }
        //  startScheduleAllClient();
        //  this->client.scheduleAllClientOneRound(this->pagesOneRound);

        this->concurrentCount = this->client->getLocationNums();

        string debugoutput;
        for(int id: this->client->getLocaitonTaskIds())
            debugoutput.append(to_string(id)+" ");
        spdlog::info(this->driverContexts->getTaskId()+" need to get data from "+ to_string(this->concurrentCount) + " Sources."+"["+debugoutput+"]");


        this->driverContexts->regRemoteSplit(regSplits);
    }



    void addInput(std::shared_ptr<DataPage> input) override {

        if(input != NULL) {
            this->inputPage = input;
        }
    }

    void generateNotes()
    {
        if(this->hasBuildTask) {
            if(!this->hasNotifyBuild) {
                if (this->driverContexts->isAllBuildCompeletedInTask()) {
                    this->client->broadcastNotes("BC");
                    this->hasNotifyBuild = true;
                    this->hasBuildTask = false;
                }
            }
        }
    }

    void resetBufferSizeByTrafficRate()
    {
        timelock.lock();
        if (this->start == NULL) {

            this->start = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());

            this->traffic = 0;
        }else {
            shared_ptr<std::chrono::system_clock::time_point> circle = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());



            double duration_millsecond = std::chrono::duration<double, std::milli>(*circle - *this->start).count();

            if(duration_millsecond > bufferTuneCircle) {

                this->client->getBuffer()->resetBufferCapacity(this->traffic);


                this->start = NULL;

            }
        }
        timelock.unlock();

    }

    shared_ptr<DataPage> provideSidewayPages() {

        if (this->waitInterTaskSync)
            this->sidewayTaskEvent->listen();

        if (!sidewayPages.empty()) {
            auto re = sidewayPages.front();
            sidewayPages.pop_front();
            return re;
        } else
            return NULL;
    }

    std::shared_ptr<DataPage> getOutput() override {

        if(this->firstOutputCall) {
            this->firstOutputCall = false;
            return NULL;
        }



        wait:

        shared_ptr<DataPage> outputData = NULL;

        if(this->upstreamFinished)
        {
            auto sidewayPage = provideSidewayPages();
            if(sidewayPage != NULL)
                return sidewayPage;
            else
                this->finished = true;
        }

        while (outputData == NULL) {

            if (this->finished) {
                outputData = this->endPage;
                return outputData;
            }

            resetBufferSizeByTrafficRate();


            if (!this->client->isFull()) {

                generateNotes();

                // this->client->scheduleAllClientOneRound(this->pagesOneRound);
                this->client->scheduleAllClientOneRoundByBufferCapacity();
            }
            outputData = this->client->pollPage();


        }

        if (outputData->isEndPage()) {


            this->endSignalCount++;

            int judge = this->endSignalCount - this->concurrentCount;


            if (judge == 0) {

                spdlog::debug("RemoteSourceOperator has received " + to_string(this->endSignalCount) + " end pages.");
                this->upstreamFinished = true;

                auto sidewayPage = provideSidewayPages();
                if(sidewayPage != NULL)
                    return sidewayPage;
                else
                    this->finished = true;

                return this->endPage;
            } else
                goto wait;
        }




        this->traffic++;
        return outputData;
    }


    bool needsInput() override {
        return false;
    }

    void abort() override
    {
        this->client->abort();
    }

    bool isFinished()
    {
        return this->finished;
    }

    string getOperatorId() override
    {
        return this->operatorId;
    }

};



#endif //OLVP_REMOTESOURCEOPERATOR_HPP
