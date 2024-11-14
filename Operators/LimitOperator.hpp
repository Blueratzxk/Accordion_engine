//
// Created by zxk on 9/23/23.
//

#ifndef OLVP_LIMITOPERATOR_HPP
#define OLVP_LIMITOPERATOR_HPP


#include "../Operators/Operator.hpp"


class LimitOperator:public Operator
{
    int elementsCount;


    bool finished;

    string name = "LimitOperator";


    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;

    int limit;
    int remainingLimit;

    shared_ptr<DriverContext> driverContext;

    int count = 0;
public:
    string getOperatorId() { return this->name; }

    LimitOperator(shared_ptr<DriverContext> driverContext,int limit) {

        this->elementsCount = 0;
        this->finished = false;
        this->driverContext = driverContext;
        this->limit = limit;
        this->remainingLimit = limit;



    }

    void addInput(std::shared_ptr<DataPage> input) override {
        if(input != NULL && input->getElementsCount() != 0) {
            this->inputPage = input;
            this->count++;
        }

    }


    void process()
    {
        if(this->remainingLimit == 0)
        {
            this->outPutPage = NULL;
            return;
        }

        if(this->inputPage->getElementsCount() <= this->remainingLimit)
        {
            this->outPutPage = inputPage;
            this->remainingLimit -= this->inputPage->getElementsCount();
        }
        else
        {
            shared_ptr<arrow::RecordBatch> slice = this->inputPage->get()->Slice(0,this->remainingLimit);
            this->outPutPage = make_shared<DataPage>(slice);
            this->remainingLimit = 0;
        }
    }


    std::shared_ptr<DataPage> getOutput() override {


        if(this->inputPage == NULL)
            return NULL;

        if(this->inputPage->isEndPage()) {
            spdlog::debug("limit process "+ to_string(this->count)+" pages");
            this->finished = true;
        }

        if(this->finished)
        {
            this->outPutPage = this->inputPage;
        }
        else
        {
            process();
        }

        this->inputPage = NULL;

        return this->outPutPage;

    }


    bool needsInput() override {
        if(this->inputPage == NULL)
            return true;
        else
            return false;
    }


    bool isFinished()
    {
        return this->finished;
    }


};



#endif //OLVP_LIMITOPERATOR_HPP
