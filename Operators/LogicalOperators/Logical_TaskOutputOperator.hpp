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

public:

    Logical_TaskOutputOperator(){

    }

    Logical_TaskOutputOperator(std::shared_ptr<OutputBuffer> outputBuffer) {
        this->outputBuffer = outputBuffer;

    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<TaskOutputOperator>(driverContext,this->outputBuffer);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<TaskOutputOperator>(driverContext,this->outputBuffer);
    }
    string getTypeId(){return name;}


};





#endif //OLVP_LOGICAL_TASKOUTPUTOPERATOR_HPP
