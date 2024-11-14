//
// Created by zxk on 5/26/23.
//

#ifndef OLVP_LOGICAL_FINALAGGREGATIONOPERATOR_HPP
#define OLVP_LOGICAL_FINALAGGREGATIONOPERATOR_HPP


#include "LogicalOperator.hpp"

#include "../FinalAggregationOperator.hpp"


class Logical_FinalAggregationOperator:public LogicalOperator {



    string name = "Logical_FinalAggregationOperator";


    AggregationDesc desc;



public:

    Logical_FinalAggregationOperator(AggregationDesc desc) {
        this->desc = desc;

    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<FinalAggregationOperator>(driverContext,this->desc);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<FinalAggregationOperator>(driverContext,this->desc);
    }
    string getTypeId(){return name;}


};




#endif //OLVP_LOGICAL_FINALAGGREGATIONOPERATOR_HPP
