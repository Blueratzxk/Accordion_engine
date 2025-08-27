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

    string operatorId;
public:

    Logical_GPUFilterOperator(string operatorId,FilterDescriptor filterDesc)  :LogicalOperator("Logical_GPUFilterOperator"){
        this->filterDesc = filterDesc;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUFilterOperator>(this->operatorId,driverContext,this->filterDesc);
    }

    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUFilterOperator>(this->operatorId,driverContext,this->filterDesc);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }


};



#endif //OLVP_LOGICAL_GPUFILTEROPERATOR_HPP
