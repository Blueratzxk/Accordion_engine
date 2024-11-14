//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_LOCALEXCHANGESOURCEOPERATOR_HPP
#define OLVP_LOCALEXCHANGESOURCEOPERATOR_HPP

#include "../Operators/Operator.hpp"
#include "LocalExchange/LocalExchange.hpp"

using namespace std;
class LocalExchangeSourceOperator:public Operator
{

    bool finished = false;

    string name = "LocalExchangeSourceOperator";

    std::shared_ptr<LocalExchangeSource> source;


    shared_ptr<DriverContext> driverContext;


public:
    string getOperatorId() { return this->name; }

    LocalExchangeSourceOperator(shared_ptr<DriverContext> driverContext,std::shared_ptr<LocalExchangeSource> source) {


        this->source = source;
        this->finished = false;
        this->driverContext = driverContext;

    }

    void addInput(std::shared_ptr<DataPage> input) override {

        spdlog::warn("localExchangeSourceOperator do not need addInput!");
        return;
    }



    std::shared_ptr<DataPage> getOutput() override {

        shared_ptr<DataPage> page = NULL;

        if(!this->finished)
            page = source->removePage();



        if(page == NULL)
            return NULL;



        if(page->isEndPage()) {
            this->finished = true;
        }


        return page;

    }


    bool needsInput() override {
        return false;
    }


    bool isFinished()
    {
        return this->finished;
    }


};


#endif //OLVP_LOCALEXCHANGESOURCEOPERATOR_HPP
