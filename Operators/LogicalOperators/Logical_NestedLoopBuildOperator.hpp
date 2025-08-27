//
// Created by zxk on 6/28/23.
//

#ifndef OLVP_LOGICAL_NESTEDLOOPBUILDOPERATOR_HPP
#define OLVP_LOGICAL_NESTEDLOOPBUILDOPERATOR_HPP



#include "LogicalOperator.hpp"

#include "../NestedLoopBuildOperator.hpp"

class Logical_NestedLoopBuildOperator:public LogicalOperator
{



private:


    string name = "Logical_NestedLoopBuildOperator";


    std::shared_ptr<NestedLoopJoinBridge> joinBridge;

    string joinId;
    string operatorId;
public:




    Logical_NestedLoopBuildOperator(string operatorId,string joinId, std::shared_ptr<NestedLoopJoinBridge> joinBridge) :LogicalOperator("Logical_NestedLoopBuildOperator"){

        this->joinBridge = joinBridge;
        this->joinId = joinId;
        this->operatorId = operatorId;

    }
    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<NestedLoopBuildOperator>(this->operatorId,joinId,driverContext,this->joinBridge);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<NestedLoopBuildOperator>(this->operatorId,joinId,driverContext,this->joinBridge);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }
};




#endif //OLVP_LOGICAL_NESTEDLOOPBUILDOPERATOR_HPP
