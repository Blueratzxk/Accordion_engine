//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_LOGICAL_LOCALEXCHANGESOURCEOPERATOR_HPP
#define OLVP_LOGICAL_LOCALEXCHANGESOURCEOPERATOR_HPP

#include "LogicalOperator.hpp"

#include "../LocalExchangeSourceOperator.hpp"

using namespace std;
class Logical_LocalExchangeSourceOperator:public LogicalOperator
{

    string name = "Logical_LocalExchangeSourceOperator";


    string operatorId;
    std::shared_ptr<LocalExchangeFactory> localExchangeFactory;

public:


    Logical_LocalExchangeSourceOperator(string operatorId,std::shared_ptr<LocalExchangeFactory> localExchangeFactory) :LogicalOperator("Logical_LocalExchangeSourceOperator") {

        this->localExchangeFactory = localExchangeFactory;
        this->operatorId = operatorId;

    }

    std::shared_ptr<LocalExchangeFactory> getLocalExchangeFactory()
    {
        return this->localExchangeFactory;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSource> source = localExchangeFactory->getLocalExchange()->getNextSource();
        return std::make_shared<LocalExchangeSourceOperator>(this->operatorId,driverContext,source);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSource> source = localExchangeFactory->getLocalExchange()->getNextSource();
        return std::make_shared<LocalExchangeSourceOperator>(this->operatorId,driverContext,source);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};


#endif //OLVP_LOGICAL_LOCALEXCHANGESOURCEOPERATOR_HPP
