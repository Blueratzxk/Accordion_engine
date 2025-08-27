//
// Created by zxk on 8/19/25.
//

#ifndef OLVP_LOGICAL_GPUPARTIALAGGREGATIONOPERATOR_HPP
#define OLVP_LOGICAL_GPUPARTIALAGGREGATIONOPERATOR_HPP



#include "../../Operators/LogicalOperators/LogicalOperator.hpp"

#include "GPUPartialAggregationOperator.hpp"


class Logical_GPUPartialAggregationOperator:public LogicalOperator {

    string name = "Logical_GPUPartialAggregationOperator";
    AggregationDesc desc;

    string operatorId;
public:

    Logical_GPUPartialAggregationOperator(string operatorId,AggregationDesc desc)  :LogicalOperator("Logical_GPUPartialAggregationOperator"){
        this->desc = desc;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUPartialAggregationOperator>(this->operatorId,driverContext,this->desc);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<GPUPartialAggregationOperator>(this->operatorId,driverContext,this->desc);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};

#endif //OLVP_LOGICAL_GPUPARTIALAGGREGATIONOPERATOR_HPP
