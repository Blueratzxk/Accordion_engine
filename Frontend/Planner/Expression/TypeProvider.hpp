//
// Created by zxk on 11/1/24.
//

#ifndef FRONTEND_TYPEPROVIDER_HPP
#define FRONTEND_TYPEPROVIDER_HPP
#include "../../Analyzer/Type.hpp"
#include <memory>
#include <map>
#include <vector>
#include <set>
#include "RowExpressionVisitor.hpp"
#include "VariableReferenceExpression.hpp"
#include "../../AstNodes/Expression/Expression.h"
#include "../../AstNodes/Expression/SymbolReference.hpp"
#include "spdlog/spdlog.h"

class TypeProvider : public enable_shared_from_this<TypeProvider> {
    map<string, shared_ptr<Type>> types;

public:
    static shared_ptr<TypeProvider> viewOf(map<string, shared_ptr<Type>> types) {
        return make_shared<TypeProvider>(types);
    }

    static shared_ptr<TypeProvider> copyOf(map<string, shared_ptr<Type>> types) {
        return make_shared<TypeProvider>(types);
    }

    static shared_ptr<TypeProvider> empty() {
        map<string, shared_ptr<Type>> types;
        return make_shared<TypeProvider>(types);
    }

    static shared_ptr<TypeProvider> fromVariables(vector<shared_ptr<VariableReferenceExpression>> variables) {
        map<string, shared_ptr<Type>> types;
        for (auto var: variables) {
            types[var->getName()] = var->getType();
        }
        return make_shared<TypeProvider>(types);
    }

    TypeProvider(map<string, shared_ptr<Type>> types) {
        this->types = types;
    }

    shared_ptr<Type> get(Expression *expression) {
        if (expression->getExpressionId() != "SymbolReference") {
            spdlog::error("TypeProvider need this expression is SymbolReference, but " + expression->getExpressionId());
            return NULL;
        }
        shared_ptr<Type> type = types[((SymbolReference *) expression)->getName()];
        return type;
    }

    set<shared_ptr<VariableReferenceExpression>> allVariables() {
        set<shared_ptr<VariableReferenceExpression>> allVars;
        for (auto var: this->types)
            allVars.insert(make_shared<VariableReferenceExpression>("0", var.first, var.second->getType()));
        return allVars;
    }

    map<string, shared_ptr<Type>> allTypes() {
        return types;
    }
};



#endif //FRONTEND_TYPEPROVIDER_HPP
