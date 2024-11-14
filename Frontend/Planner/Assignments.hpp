//
// Created by zxk on 10/31/24.
//

#ifndef FRONTEND_ASSIGNMENTS_HPP
#define FRONTEND_ASSIGNMENTS_HPP

#include "Expression/VariableReferenceExpression.hpp"
#include <map>
class Assignments
{

map<shared_ptr<VariableReferenceExpression>, shared_ptr<RowExpression>> assignments;
list<shared_ptr<VariableReferenceExpression>> outputs;
public:
    Assignments(map<shared_ptr<VariableReferenceExpression>, shared_ptr<RowExpression>> assignments){
        this->assignments = assignments;

        for(auto assignment : this->assignments)
            outputs.push_back(assignment.first);
    }


    map<shared_ptr<VariableReferenceExpression>, shared_ptr<RowExpression>> getMap(){return this->assignments;}

    list<shared_ptr<VariableReferenceExpression>> getOutputs(){return this->outputs;}
};

class AssignmentsBuilder : public enable_shared_from_this<AssignmentsBuilder>{

    map<shared_ptr<VariableReferenceExpression>, shared_ptr<RowExpression>> assignments;

public:
    shared_ptr<AssignmentsBuilder> putAll(shared_ptr<Assignments> assignments) {
        return putAll(assignments->getMap());
    }

    shared_ptr<AssignmentsBuilder>
    putAll(map<shared_ptr<VariableReferenceExpression>, shared_ptr<RowExpression>> assignments) {
        for (auto assignment: assignments) {
            put(assignment.first, assignment.second);
        }
        return shared_from_this();
    }

    shared_ptr<AssignmentsBuilder>
    put(shared_ptr<VariableReferenceExpression> variable, shared_ptr<RowExpression> expression) {
        if (assignments.contains(variable)) {
            shared_ptr<RowExpression> assignment = assignments[variable];
            //if (!assignment.equals(expression)) {
            //   throw new IllegalStateException(format("Variable %s already has assignment %s, while adding %s", variable, assignment, expression));
            //}
        }
        assignments[variable] = expression;
        return shared_from_this();
    }



    shared_ptr<Assignments> build() {
        return make_shared<Assignments>(this->assignments);
    }
};


#endif //FRONTEND_ASSIGNMENTS_HPP
