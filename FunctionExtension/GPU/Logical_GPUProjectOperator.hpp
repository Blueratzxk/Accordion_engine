//
// Created by zxk on 8/17/25.
//

#ifndef OLVP_LOGICAL_GPUPROJECTOPERATOR_HPP
#define OLVP_LOGICAL_GPUPROJECTOPERATOR_HPP




#include "../../Operators/LogicalOperators/LogicalOperator.hpp"


#include "GPUProjectOperator.hpp"

class Logical_GPUProjectOperator:public LogicalOperator
{

    string name = "Logical_GPUProjectOperator";

    ProjectAssignments assignments;

    string operatorId;
public:

    Logical_GPUProjectOperator(string operatorId,ProjectAssignments assignments) :LogicalOperator("Logical_GPUProjectOperator") {
        this->assignments = assignments;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUProjectOperator>(this->operatorId,driverContext,this->assignments);
    }

    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUProjectOperator>(this->operatorId,driverContext,this->assignments);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }



};



#endif //OLVP_LOGICAL_GPUPROJECTOPERATOR_HPP
