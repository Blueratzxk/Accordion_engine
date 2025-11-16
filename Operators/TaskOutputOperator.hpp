//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_TASKOUTPUTOPERATOR_HPP
#define OLVP_TASKOUTPUTOPERATOR_HPP

#include "../Operators/Operator.hpp"
#include "LocalExchange/LocalExchange.hpp"

#include "../Execution/Buffer/OutputBuffer.hpp"

using namespace std;

class TaskOutputOperator:public Operator
{

    bool finished;

    std::shared_ptr<OutputBuffer> outputBuffer;

    string name = "TaskOutputOperator";


    std::shared_ptr<DataPage> inputPage = NULL;

    shared_ptr<DriverContext> driverContext;

    bool ignoreTrafficControl = false;

    string operatorId;

public:


    TaskOutputOperator(string operatorId,shared_ptr<DriverContext> driverContext,std::shared_ptr<OutputBuffer> outputBuffer, bool ignoreTrafficControl = false) : Operator("TaskOutputOperator") {


        this->outputBuffer = outputBuffer;
        this->finished = false;
        this->driverContext = driverContext;
        this->outputBuffer->regOutputOperator();
        this->ignoreTrafficControl = ignoreTrafficControl;
        this->operatorId = operatorId;
    }


    void addInput(std::shared_ptr<DataPage> input) override {


        if(this->finished) {
            return;
        }

        if(input->isEndPage())
            this->finished = true;

        this->inputPage = input;

        if(this->inputPage != NULL)
            this->inputPage = inputPage;


        this->outputBuffer->enqueue({this->inputPage });
     //   this->driverContext->addTupleCountForTask(this->inputPage->getElementsCount());

        while(!this->ignoreTrafficControl && this->outputBuffer->isFull())
            ;

        if(this->outputBuffer->isEmpty())
        {
            this->outputBuffer->changeBufferSize();
        }


        this->inputPage = NULL;

    }



    std::shared_ptr<DataPage> getOutput() override {

     return NULL;

    }

    bool needsInput() override {
        return true;
    }

    bool isFinished()
    {
        return this->finished;
    }

    string getOperatorId() override
    {
        return this->operatorId;
    }

    void abort() override {
        this->finished = true;
    }

};





#endif //OLVP_TASKOUTPUTOPERATOR_HPP
