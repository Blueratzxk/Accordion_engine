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

public:

    Logical_GPUPartialAggregationOperator(AggregationDesc desc) {
        this->desc = desc;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUPartialAggregationOperator>(driverContext,this->desc);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<GPUPartialAggregationOperator>(driverContext,this->desc);
    }
    string getTypeId(){return name;}


};

#endif //OLVP_LOGICAL_GPUPARTIALAGGREGATIONOPERATOR_HPP
