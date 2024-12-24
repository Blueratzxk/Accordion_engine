//
// Created by zxk on 6/28/23.
//

#ifndef OLVP_NESTEDLOOPBUILDOPERATOR_HPP
#define OLVP_NESTEDLOOPBUILDOPERATOR_HPP



#include "../Operators/Operator.hpp"

#include "../Page/Channel.hpp"
#include "../Utils/ArrowArrayBuilder.hpp"
#include "Join/NestedLoopJoin/NestedLoopJoinPagesSupplier.hpp"
#include "../Execution/Task/Context/DriverContext.h"

class NestedLoopBuildOperator:public Operator
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

    string name = "NestedLoopBuildOperator";

    bool sendEndPage = false;


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;


    std::shared_ptr<arrow::Schema> inputSchema = NULL;

    vector<int> outputChannels;

    vector<shared_ptr<DataPage>> pages;

    State state= State::CONSUMING_INPUT;

    std::shared_ptr<NestedLoopJoinBridge> joinBridge;


    shared_ptr<std::chrono::system_clock::time_point> firstStartBuildTime = NULL;
    shared_ptr<std::chrono::system_clock::time_point> lastBuildFinishedTime = NULL;
    shared_ptr<std::chrono::system_clock::time_point> buildComputingStartTime = NULL;

    string joinId;

    shared_ptr<DriverContext> driverContext;
public:



    string getOperatorId() { return this->name; }

    NestedLoopBuildOperator(string joinId,shared_ptr<DriverContext> driverContext, std::shared_ptr<NestedLoopJoinBridge> joinBridge) {

        this->finished = false;
        this->joinBridge = joinBridge;

        this->joinId = joinId;
        this->outputChannels = outputChannels;

        this->driverContext = driverContext;

    }
    State getState()
    {
        return this->state;
    }




    void addInput(std::shared_ptr<DataPage> input) override {
        if(input != NULL && !input->isEndPage()) {
            this->inputPage = input;
            this->pages.push_back(input);

            if(this->firstStartBuildTime == NULL)
                this->firstStartBuildTime = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());
        }
        else
        {
            if(!this->finished) {
                this->finishBuild();
                spdlog::debug("NestLoopJoin Build Finished!");
            }
            this->finished = true;
        }

    }




    void finishInput()
    {
        this->joinBridge->setPages(make_shared<NestedLoopJoinPages>(pages));

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





#endif //OLVP_NESTEDLOOPBUILDOPERATOR_HPP
