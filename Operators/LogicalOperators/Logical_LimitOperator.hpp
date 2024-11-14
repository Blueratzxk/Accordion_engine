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

public:
    string getOperatorId() { return this->name; }

    Logical_LimitOperator(int limit) {
        this->limit = limit;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<LimitOperator>(driverContext,this->limit);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<LimitOperator>(driverContext,this->limit);
    }
    string getTypeId(){return name;}



};


#endif //OLVP_LOGICAL_LIMITOPERATOR_HPP
