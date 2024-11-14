//
// Created by zxk on 10/21/24.
//

#ifndef FRONTEND_VARIABLEALLOCATOR_HPP
#define FRONTEND_VARIABLEALLOCATOR_HPP

#include <map>
#include <vector>
#include <algorithm>
#include "Expression/TypeProvider.hpp"
#include "../Planner/Expression/RowExpressionNodeAllocator.hpp"

class VariableAllocator {
    map<string, shared_ptr<Type>> variables;
    int nextId = 0;

public:
    VariableAllocator() {

    }

    VariableAllocator(vector<shared_ptr<VariableReferenceExpression>> initial) {

        for (auto v: initial)
            variables[v->getName()] = v->getType();
    }


    bool isDigit(const std::string &str) {
        return !str.empty() && std::all_of(str.begin(), str.end(), ::isdigit);
    }


    shared_ptr<VariableReferenceExpression> newVariable(string nameHint, shared_ptr<Type> type) {
        return newVariable("", nameHint, type);
    }

    shared_ptr<VariableReferenceExpression>
    newVariable(string nameHint, shared_ptr<Type> type, string suffix) {
        return newVariable("", nameHint, type);
    }

    shared_ptr<VariableReferenceExpression>
    newVariable(string sourceLocation, string nameHint, shared_ptr<Type> type) {
        return newVariable(sourceLocation, nameHint, type, "");
    }

    shared_ptr<VariableReferenceExpression>
    newVariable(string sourceLocation, string nameHint, shared_ptr<Type> type, string suffix) {


        for (int i = 0; i < nameHint.size(); i++) {
            nameHint[i] = tolower(nameHint[i]);
        }


        // don't strip the tail if the only _ is the first character
        int index = nameHint.find('_');
        if (index > 0) {
            string tail = nameHint.substr(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (isDigit(tail) || index == nameHint.length() - 1) {
                nameHint = nameHint.substr(0, index);
            }
        }

        string unique = nameHint;

        if (suffix != "") {
            unique = unique + "$" + suffix;
        }
        // remove special characters for other special serde


        string attempt = unique;

        while (variables.count(attempt) > 0)
            attempt = unique + "_" + to_string(getNextId());

        variables[attempt] = type;

        return make_shared<VariableReferenceExpression>(sourceLocation, attempt, type->getType());
    }

    int getNextId() {
        return this->nextId++;
    }
    shared_ptr<TypeProvider> getTypes()
    {
        return TypeProvider::viewOf(variables);
    }
    map<string, shared_ptr<Type>> getVariables() {
        return this->variables;
    }

    shared_ptr<VariableReferenceExpression> newVariable(shared_ptr<RowExpression> expression, string suffix)
    {
        string nameHint = "expr";
        if (expression->getExpressionName() == "VariableReferenceExpression") {
            nameHint = dynamic_pointer_cast<VariableReferenceExpression>(expression)->getName();
        }
        else if (expression->getExpressionName() == "CallExpression") {
            nameHint = dynamic_pointer_cast<CallExpression>(expression)->getDisplayName();
        }
        return newVariable(expression->getSourceLocation(), nameHint, expression->getType(), suffix);
    }

shared_ptr<VariableReferenceExpression> getVariableReferenceExpression(string sourceLocation, string name)
    {
        return make_shared<VariableReferenceExpression>(sourceLocation, name, variables[name]->getType());
    }

    bool hasSpliter(string name)
    {
        if(name.rfind('_')!= name.npos || name.rfind('$')!= name.npos) {
            int index;
            if(name.rfind('_')!= name.npos)
                index = name.rfind('_');
            if(name.rfind('$')!= name.npos)
                index = name.rfind("$");

            if(index+1 > name.size())
                return false;

            string sub = name.substr(index+1,name.npos);

            bool isdigit = isDigit(sub);
            if(isdigit)
                return true;
            else if(sub == "gid")
                return true;

            return false;

        }
        else
            return false;
    }
    string getSpliter(string name)
    {
        if( name.rfind('$')!= name.npos)
            return "$";
        if(name.rfind('_')!= name.npos)
            return "_";

        return "";
    }
    string extractPrefix(string name,string spliter)
    {
        int index = name.rfind(spliter);
        if(index != name.npos)
            return name.substr(0,index);
        return "";
    }

    string getNameHint(Expression *expression)
    {
        string nameHint = "expr";
        if (expression->getExpressionId() == "Identifier") {
            nameHint = ((Identifier*) expression)->getValue();
        }
        else if (expression->getExpressionId() == "FunctionCall") {
            nameHint = ((FunctionCall*) expression)->getFuncName();
        }
        else if (expression->getExpressionId() == "SymbolReference") {
            nameHint = ((SymbolReference*) expression)->getName();
        }

        return nameHint;
    }


};


#endif //FRONTEND_VARIABLEALLOCATOR_HPP
