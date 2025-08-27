//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_LOGICAL_PARTIALAGGREGATIONOPERATOR_HPP
#define OLVP_LOGICAL_PARTIALAGGREGATIONOPERATOR_HPP


#include "LogicalOperator.hpp"

#include "../PartialAggregationOperator.hpp"


class Logical_PartialAggregationOperator:public LogicalOperator {



    string name = "Logical_PartialAggregationOperator";


    AggregationDesc desc;
    string operatorId;

public:

    Logical_PartialAggregationOperator(string operatorId,AggregationDesc desc):LogicalOperator("Logical_PartialAggregationOperator") {
        this->desc = desc;
        this->operatorId = operatorId;
    }

    AggregationDesc getDesc()
    {
        return this->desc;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<PartialAggregationOperator>(this->operatorId,driverContext,this->desc);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<PartialAggregationOperator>(this->operatorId,driverContext,this->desc);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};

#endif //OLVP_LOGICAL_PARTIALAGGREGATIONOPERATOR_HPP
