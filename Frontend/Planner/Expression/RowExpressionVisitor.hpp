//
// Created by zxk on 11/5/24.
//

#ifndef FRONTEND_ROWEXPRESSIONVISITOR_HPP
#define FRONTEND_ROWEXPRESSIONVISITOR_HPP


#include "RowExpression.hpp"

class CallExpression;
class InputReferenceExpression;
class ConstantExpression;
class VariableReferenceExpression;

class RowExpressionVisitor {

public:

    RowExpressionVisitor(){}


    void* Visit(RowExpression* node,void *context) {
        return node->accept(this,context);
    }

    virtual void *VisitExpression(RowExpression *expression, void *context)  = 0;
    virtual void *VisitCall(CallExpression *call, void * context) = 0;

    virtual void *VisitInputReference(InputReferenceExpression *reference, void * context) = 0;

    virtual void *VisitConstant(ConstantExpression *literal, void * context)= 0;

    virtual void *VisitVariableReference(VariableReferenceExpression *reference, void * context)= 0;
};

#endif //FRONTEND_ROWEXPRESSIONVISITOR_HPP
