//
// Created by zxk on 8/16/25.
//

#ifndef OLVP_LOGICAL_GPUFILTEROPERATOR_HPP
#define OLVP_LOGICAL_GPUFILTEROPERATOR_HPP



#include "../../Operators/LogicalOperators/LogicalOperator.hpp"


#include "GPUFilterOperator.hpp"

class Logical_GPUFilterOperator:public LogicalOperator
{

    string name = "Logical_GPUFilterOperator";

    FilterDescriptor filterDesc;


public:

    Logical_GPUFilterOperator(FilterDescriptor filterDesc) {
        this->filterDesc = filterDesc;

    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUFilterOperator>(driverContext,this->filterDesc);
    }

    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUFilterOperator>(driverContext,this->filterDesc);
    }

    string getTypeId(){return this->name;}



};



#endif //OLVP_LOGICAL_GPUFILTEROPERATOR_HPP
