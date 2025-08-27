//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_LOGICAL_LOCALEXCHANGESINKOPERATOR_HPP
#define OLVP_LOGICAL_LOCALEXCHANGESINKOPERATOR_HPP


#include "LogicalOperator.hpp"

#include "../LocalExchangeSinkOperator.hpp"

using namespace std;

class Logical_LocalExchangeSinkOperator:public LogicalOperator
{


    string name = "Logical_LocalExchangeSinkOperator";

    std::shared_ptr<LocalExchangeFactory> localExchangeFactory;

    string operatorId;
public:


    Logical_LocalExchangeSinkOperator(string operatorId,std::shared_ptr<LocalExchangeFactory> localExchangeFactory)  :LogicalOperator("Logical_LocalExchangeSinkOperator"){

        this->localExchangeFactory = localExchangeFactory;
        this->operatorId = operatorId;

    }
    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSink> sink = this->localExchangeFactory->getLocalExchange()->createSink();
        return std::make_shared<LocalExchangeSinkOperator>(this->operatorId,driverContext,sink);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSink> sink = this->localExchangeFactory->getLocalExchange()->createSink();
        return std::make_shared<LocalExchangeSinkOperator>(this->operatorId,driverContext,sink);
    }



    string getExchangeType()
    {
        return this->localExchangeFactory->getExchangeType();
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};



#endif //OLVP_LOGICAL_LOCALEXCHANGESINKOPERATOR_HPP
