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

    bool interTaskDataSync = false;

public:

    Logical_FinalAggregationOperator(AggregationDesc desc,bool interTaskDataSync) {
        this->desc = desc;
        this->interTaskDataSync = interTaskDataSync;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        auto op = std::make_shared<FinalAggregationOperator>(driverContext,this->desc);
        if(this->interTaskDataSync)
            op->waitInterTaskDataSync();
        return op;
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        auto op = std::make_shared<FinalAggregationOperator>(driverContext,this->desc);
        if(this->interTaskDataSync)
            op->waitInterTaskDataSync();
        return op;
    }
    string getTypeId(){return name;}


};




#endif //OLVP_LOGICAL_FINALAGGREGATIONOPERATOR_HPP
