//
// Created by zxk on 9/23/23.
//

#ifndef OLVP_LOGICAL_LIMITOPERATOR_HPP
#define OLVP_LOGICAL_LIMITOPERATOR_HPP


#include "LogicalOperator.hpp"

#include "../LimitOperator.hpp"

class Logical_LimitOperator:public LogicalOperator
{


    string name = "Logical_LimitOperator";

    int limit;

    string operatorId;

public:


    Logical_LimitOperator(string operatorId,int limit) :LogicalOperator("Logical_LimitOperator") {
        this->limit = limit;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<LimitOperator>(this->operatorId,driverContext,this->limit);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<LimitOperator>(this->operatorId,driverContext,this->limit);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }


};


#endif //OLVP_LOGICAL_LIMITOPERATOR_HPP
