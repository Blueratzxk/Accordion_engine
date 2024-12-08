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



public:
    string getOperatorId() { return this->name; }

    TaskOutputOperator(shared_ptr<DriverContext> driverContext,std::shared_ptr<OutputBuffer> outputBuffer) {


        this->outputBuffer = outputBuffer;
        this->finished = false;
        this->driverContext = driverContext;
        this->outputBuffer->regOutputOperator();
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

        while(this->outputBuffer->isFull())
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


};





#endif //OLVP_TASKOUTPUTOPERATOR_HPP
