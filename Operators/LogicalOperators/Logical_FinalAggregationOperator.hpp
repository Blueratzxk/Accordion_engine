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

    string operatorId;

public:

    Logical_FinalAggregationOperator(string operatorId,AggregationDesc desc,bool interTaskDataSync):LogicalOperator("Logical_FinalAggregationOperator")  {
        this->desc = desc;
        this->interTaskDataSync = interTaskDataSync;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        auto op = std::make_shared<FinalAggregationOperator>(this->operatorId,driverContext,this->desc);
        if(this->interTaskDataSync)
            op->waitSidewayDataSync();
        return op;
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        auto op = std::make_shared<FinalAggregationOperator>(this->operatorId,driverContext,this->desc);
        if(this->interTaskDataSync)
            op->waitSidewayDataSync();
        return op;
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};




#endif //OLVP_LOGICAL_FINALAGGREGATIONOPERATOR_HPP
