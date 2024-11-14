//
// Created by zxk on 10/21/24.
//

#ifndef FRONTEND_CALLEXPRESSION_HPP
#define FRONTEND_CALLEXPRESSION_HPP

#include "RowExpression.hpp"
#include "../../Analyzer/FunctionCalls/FunctionHandle.hpp"
#include "../../Analyzer/Type.hpp"
#include <list>

using namespace std;

class CallExpression : public RowExpression {
    string displayName;
    shared_ptr<FunctionHandle> functionHandle;
    shared_ptr<Type> returnType;
    list<shared_ptr<RowExpression>> arguments;

public:

    CallExpression(string sourceLocation,string displayName,
                   shared_ptr<FunctionHandle> functionHandle,
                   shared_ptr<Type> returnType,
                   list<shared_ptr<RowExpression>> arguments): RowExpression(sourceLocation,"CallExpression"){

        this->displayName = displayName;
        this->functionHandle = functionHandle;
        this->returnType = returnType;
        this->arguments = arguments;
    }


    string getDisplayName(){return this->displayName;}
    shared_ptr<FunctionHandle> getFunctionHandle(){return this->functionHandle;}
    shared_ptr<Type> getReturnType(){return this->returnType;}
    list<shared_ptr<RowExpression>> getArguments(){return this->arguments;}

    shared_ptr<Type> getType() override{
        return returnType;
    }

    std::string getNameOrValue() override
    {
        return this->displayName;
    }

    void * accept(RowExpressionVisitor *visitor, void *context) override
    {
        return visitor->VisitCall(this,context);
    }

};
#endif //FRONTEND_CALLEXPRESSION_HPP
