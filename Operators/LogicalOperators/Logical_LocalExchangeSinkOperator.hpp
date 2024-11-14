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


public:
    string getOperatorId() { return this->name; }

    Logical_LocalExchangeSinkOperator(std::shared_ptr<LocalExchangeFactory> localExchangeFactory) {

        this->localExchangeFactory = localExchangeFactory;

    }
    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSink> sink = this->localExchangeFactory->getLocalExchange()->createSink();
        return std::make_shared<LocalExchangeSinkOperator>(driverContext,sink);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSink> sink = this->localExchangeFactory->getLocalExchange()->createSink();
        return std::make_shared<LocalExchangeSinkOperator>(driverContext,sink);
    }
    string getTypeId(){return name;}


    string getExchangeType()
    {
        return this->localExchangeFactory->getExchangeType();
    }

};



#endif //OLVP_LOGICAL_LOCALEXCHANGESINKOPERATOR_HPP
