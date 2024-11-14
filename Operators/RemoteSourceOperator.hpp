//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_REMOTESOURCEOPERATOR_HPP
#define OLVP_REMOTESOURCEOPERATOR_HPP


#include "../Operators/Operator.hpp"
#include "../Web/ArrowRPC/DataPageRPCBuffer.hpp"
#include "../Web/ArrowRPC/RPCClient.hpp"

//class DriverContext;
using namespace  std;
class RemoteSourceOperator:public Operator
{

    bool finished;

    std::shared_ptr<DataPage> inputPage;

    string name = "RemoteSourceOperator";


    shared_ptr<RPCClient> client;

    int endSignalCount = 0;
    atomic<int> concurrentCount = 0 ;


    atomic<bool> abortTransmission = false;



    mutex lock;


    shared_ptr<DriverContext> driverContexts;



    bool hasBuildTask = false;
    bool hasNotifyBuild = false;

    mutex timelock;

    int traffic = 0;
    int maxTraffic = 0;
    shared_ptr<std::chrono::system_clock::time_point> start = NULL;
    int bufferTuneCircle = 500; //ms

public:
    string getOperatorId() { return this->name; }

    RemoteSourceOperator(shared_ptr<DriverContext> driverContext) {

        this->finished = false;
        this->client = make_shared<RPCClient>();
        this->driverContexts = driverContext;
        this->hasBuildTask = this->driverContexts->hasBuildTask();
        this->client->addDriverContext(this->driverContexts);

    }


    void addSources(set<shared_ptr<Split>> Splits)
    {
        if(abortTransmission)
            return;

        if(this->concurrentCount < Splits.size())
            this->concurrentCount = Splits.size();
        for(auto split : Splits)
        {
            this->client->addLocation(static_pointer_cast<RemoteSplit>(split->getConnectorSplit()));
        }
        //  startScheduleAllClient();
        //  this->client.scheduleAllClientOneRound(this->pagesOneRound);


        this->driverContexts->regRemoteSplit(Splits);
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

    std::shared_ptr<DataPage> getOutput() override {

        wait:

        shared_ptr<DataPage> outputData = NULL;


        while (outputData == NULL) {

            if (this->finished) {
                outputData = DataPage::getEndPage();
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
                this->finished = true;
                return DataPage::getEndPage();
            } else
                goto wait;
        }




        this->traffic++;
        return outputData;
    }


    bool needsInput() override {
        return false;
    }

    void abort()
    {
        this->client->abort();
    }

    bool isFinished()
    {
        return this->finished;
    }


};



#endif //OLVP_REMOTESOURCEOPERATOR_HPP
