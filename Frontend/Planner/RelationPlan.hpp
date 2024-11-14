//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_RELATIONPLAN_HPP
#define FRONTEND_RELATIONPLAN_HPP

#include "../PlanNode/PlanNode.hpp"
#include "Expression/VariableReferenceExpression.hpp"
#include "../Analyzer/Scope.hpp"
#include "spdlog/spdlog.h"
class RelationPlan {

    PlanNode *root;
    vector<shared_ptr<VariableReferenceExpression>> fieldMappings;
    Scope *scope;
public:
    RelationPlan(PlanNode *root, Scope *scope, vector<shared_ptr<VariableReferenceExpression>> fieldMappings) {
        this->root = root;
        this->scope = scope;
        this->fieldMappings = fieldMappings;

        if (getAllFieldCount(scope) != fieldMappings.size()) {
            spdlog::error("Number of outputs doesn't match number of fields in scopes tree");
        }
    }

    shared_ptr<VariableReferenceExpression> getVariable(int fieldIndex) {
        return fieldMappings[fieldIndex];
    }

    int getAllFieldCount(Scope *root) {
        int allFieldCount = 0;
        Scope *current = root;
        while (current != NULL) {
            allFieldCount += current->getRelationType()->getAllFieldCount();
            current = current->getLocalParent();
        }
        return allFieldCount;
    }

    bool hasField(shared_ptr<VariableReferenceExpression> variable)
    {
        for(auto var : this->fieldMappings)
        {
            if(variable->getName() == var->getName())
                return true;
        }
        return false;
    }

    PlanNode *getRoot() {
        return root;
    }

    vector<shared_ptr<VariableReferenceExpression>> getFieldMappings() {
        return fieldMappings;
    }

    shared_ptr<RelationType> getDescriptor() {
        return scope->getRelationType();
    }

    Scope *getScope() {
        return scope;
    }
};


#endif //FRONTEND_RELATIONPLAN_HPP
