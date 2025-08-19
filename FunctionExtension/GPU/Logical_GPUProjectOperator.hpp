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


public:

    Logical_GPUProjectOperator(ProjectAssignments assignments) {
        this->assignments = assignments;

    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUProjectOperator>(driverContext,this->assignments);
    }

    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUProjectOperator>(driverContext,this->assignments);
    }

    string getTypeId(){return this->name;}



};



#endif //OLVP_LOGICAL_GPUPROJECTOPERATOR_HPP
