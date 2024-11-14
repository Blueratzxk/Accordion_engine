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


public:

    Logical_FilterOperator(FilterDescriptor filterDesc) {
        this->filterDesc = filterDesc;

    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<FilterOperator>(driverContext,this->filterDesc);
    }

    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<FilterOperator>(driverContext,this->filterDesc);
    }

    string getTypeId(){return this->name;}



};


#endif //OLVP_LOGICAL_FILTEROPERATOR_HPP
