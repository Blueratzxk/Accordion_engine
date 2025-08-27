//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_LOCALEXCHANGESINKOPERATOR_HPP
#define OLVP_LOCALEXCHANGESINKOPERATOR_HPP



#include "../Operators/Operator.hpp"
#include "LocalExchange/LocalExchange.hpp"

using namespace std;

class LocalExchangeSinkOperator:public Operator
{

    bool finished;

    string name = "LocalExchangeSinkOperator";

    std::shared_ptr<LocalExchangeSink> sink;

    shared_ptr<DriverContext> driverContext;

    int pageCounter = 0;
    string operatorId;
public:


    LocalExchangeSinkOperator(string operatorId,shared_ptr<DriverContext> driverContext,std::shared_ptr<LocalExchangeSink> sink):Operator("LocalExchangeSinkOperator") {

        this->sink = sink;
        this->finished = false;
        this->driverContext = driverContext;
        this->operatorId = operatorId;
    }

    void addInput(std::shared_ptr<DataPage> input) override {

        if(input->isEndPage()) {
            this->finished = true;
            spdlog::info("Sink operator process "+ to_string(this->pageCounter) + " pages");
        }
        sink->addPage(input);
        pageCounter++;
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
};



#endif //OLVP_LOCALEXCHANGESINKOPERATOR_HPP
