//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_LOGICAL_FILTEROPERATOR_HPP
#define OLVP_LOGICAL_FILTEROPERATOR_HPP

#include "LogicalOperator.hpp"

#include "../FilterOperator.hpp"

class Logical_FilterOperator:public LogicalOperator
{

    string name = "Logical_FilterOperator";

    FilterDescriptor filterDesc;

    string operatorId;

public:

    Logical_FilterOperator(string operatorId,FilterDescriptor filterDesc) :LogicalOperator("Logical_FilterOperator") {
        this->filterDesc = filterDesc;
        this->operatorId = operatorId;

    }

    FilterDescriptor getFilterDesc()
    {
        return this->filterDesc;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<FilterOperator>(this->operatorId,driverContext,this->filterDesc);
    }

    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<FilterOperator>(this->operatorId,driverContext,this->filterDesc);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};


#endif //OLVP_LOGICAL_FILTEROPERATOR_HPP
