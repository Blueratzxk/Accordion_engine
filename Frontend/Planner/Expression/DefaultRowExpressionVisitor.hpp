//
// Created by zxk on 11/5/24.
//

#ifndef FRONTEND_DEFAULTROWEXPRESSIONVISITOR_HPP
#define FRONTEND_DEFAULTROWEXPRESSIONVISITOR_HPP


#include "RowExpressionVisitor.hpp"
#include "CallExpression.hpp"
#include "InputReferenceExpression.hpp"
#include "ConstantExpression.hpp"
#include "VariableReferenceExpression.hpp"

class DefaultRowExpressionVisitor : public RowExpressionVisitor{

public:

    void *VisitExpression(RowExpression *expression, void *context)override
    {
        return nullptr;
    }
    void *VisitCall(CallExpression *call, void * context)  override  {

        return VisitExpression(call,context);
    }

    void *VisitInputReference(InputReferenceExpression *reference, void * context) override{
        return VisitExpression(reference,context);
    }

    void *VisitConstant(ConstantExpression *literal, void * context)  override  {
        return VisitExpression(literal,context);
    }

    void *VisitVariableReference(VariableReferenceExpression *reference, void * context) override{
        return VisitExpression(reference,context);
    }


};


#endif //FRONTEND_DEFAULTROWEXPRESSIONVISITOR_HPP
