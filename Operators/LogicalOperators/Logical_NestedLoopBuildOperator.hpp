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
public:



    string getOperatorId() { return this->name; }

    Logical_NestedLoopBuildOperator(string joinId, std::shared_ptr<NestedLoopJoinBridge> joinBridge) {

        this->joinBridge = joinBridge;
        this->joinId = joinId;

    }
    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<NestedLoopBuildOperator>(joinId,driverContext,this->joinBridge);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<NestedLoopBuildOperator>(joinId,driverContext,this->joinBridge);
    }

    string getTypeId(){return name;}



};




#endif //OLVP_LOGICAL_NESTEDLOOPBUILDOPERATOR_HPP
