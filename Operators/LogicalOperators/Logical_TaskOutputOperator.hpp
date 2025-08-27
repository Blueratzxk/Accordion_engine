//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_LOGICAL_TASKOUTPUTOPERATOR_HPP
#define OLVP_LOGICAL_TASKOUTPUTOPERATOR_HPP

#include "LogicalOperator.hpp"

#include "../TaskOutputOperator.hpp"

using namespace std;

class Logical_TaskOutputOperator:public LogicalOperator
{

    std::shared_ptr<OutputBuffer> outputBuffer = NULL;
    string name = "Logical_TaskOutputOperator";
    bool noTrafficControl = false;

    string operatorId;
public:

    Logical_TaskOutputOperator(string operatorId):LogicalOperator("Logical_TaskOutputOperator"){
        this->operatorId = operatorId;
    }

    Logical_TaskOutputOperator(string operatorId,std::shared_ptr<OutputBuffer> outputBuffer,bool noTrafficControl = false) :LogicalOperator("Logical_TaskOutputOperator"){
        this->outputBuffer = outputBuffer;
        this->noTrafficControl = noTrafficControl;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<TaskOutputOperator>(this->operatorId,driverContext,this->outputBuffer,this->noTrafficControl);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<TaskOutputOperator>(this->operatorId,driverContext,this->outputBuffer,this->noTrafficControl);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }
};





#endif //OLVP_LOGICAL_TASKOUTPUTOPERATOR_HPP
