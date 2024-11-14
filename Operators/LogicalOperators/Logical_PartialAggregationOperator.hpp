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


public:

    Logical_PartialAggregationOperator(AggregationDesc desc) {
        this->desc = desc;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<PartialAggregationOperator>(driverContext,this->desc);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<PartialAggregationOperator>(driverContext,this->desc);
    }
    string getTypeId(){return name;}


};

#endif //OLVP_LOGICAL_PARTIALAGGREGATIONOPERATOR_HPP
