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


public:
    string getOperatorId() { return this->name; }

    Logical_ProjectOperator(ProjectAssignments assignments) {

        this->assignments = assignments;


    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<ProjectOperator>(driverContext,this->assignments);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<ProjectOperator>(driverContext,this->assignments);
    }
    string getTypeId(){return name;}



};
#endif //OLVP_LOGICAL_PROJECTOPERATOR_HPP
