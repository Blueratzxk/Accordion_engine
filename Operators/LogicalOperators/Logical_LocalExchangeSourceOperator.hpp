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



    std::shared_ptr<LocalExchangeFactory> localExchangeFactory;

public:


    Logical_LocalExchangeSourceOperator(std::shared_ptr<LocalExchangeFactory> localExchangeFactory) {

        this->localExchangeFactory = localExchangeFactory;

    }

    std::shared_ptr<LocalExchangeFactory> getLocalExchangeFactory()
    {
        return this->localExchangeFactory;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSource> source = localExchangeFactory->getLocalExchange()->getNextSource();
        return std::make_shared<LocalExchangeSourceOperator>(driverContext,source);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        std::shared_ptr<LocalExchangeSource> source = localExchangeFactory->getLocalExchange()->getNextSource();
        return std::make_shared<LocalExchangeSourceOperator>(driverContext,source);
    }
    string getTypeId(){return name;}



};


#endif //OLVP_LOGICAL_LOCALEXCHANGESOURCEOPERATOR_HPP
