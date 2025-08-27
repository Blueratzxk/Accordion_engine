//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_LOGICAL_PROJECTOPERATOR_HPP
#define OLVP_LOGICAL_PROJECTOPERATOR_HPP

#include "LogicalOperator.hpp"

#include "../ProjectOperator.hpp"

class Logical_ProjectOperator:public LogicalOperator
{


    string name = "Logical_ProjectOperator";



    ProjectAssignments assignments;
    string operatorId;

public:


    Logical_ProjectOperator(string operatorId,ProjectAssignments assignments):LogicalOperator("Logical_ProjectOperator") {

        this->assignments = assignments;
        this->operatorId = operatorId;

    }

    ProjectAssignments getAssignments()
    {
        return this->assignments;
    }
    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<ProjectOperator>(this->operatorId,driverContext,this->assignments);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<ProjectOperator>(this->operatorId,driverContext,this->assignments);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};
#endif //OLVP_LOGICAL_PROJECTOPERATOR_HPP
