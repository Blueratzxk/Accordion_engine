//
// Created by zxk on 11/3/24.
//

#ifndef FRONTEND_EXPRESSIONUTILS_HPP
#define FRONTEND_EXPRESSIONUTILS_HPP

#include "ComparisonExpression.h"
#include "ArithmeticBinaryExpression.h"
#include "LogicalBinaryExpression.h"

class ExpressionUtils
{
public:
    ExpressionUtils()
    {

    }

    static list<Expression *>extractConjuncts(Expression *expression)
    {
        return extractPredicates(LogicalBinaryExpression::AND,expression);
    }

    static list<Expression *> extractPredicates(LogicalBinaryExpression *expression)
    {
        return extractPredicates(expression->getOperatorType(), expression);
    }

    static list<Expression*> extractPredicates(LogicalBinaryExpression::Operator operatorType, Expression *expression)
    {
        list<Expression*> predicates;
        if (expression->getExpressionId() == "LogicalBinaryExpression" && ((LogicalBinaryExpression*) expression)->getOperatorType() == operatorType) {
            auto logicalBinaryExpression = (LogicalBinaryExpression *) expression;

            for (auto pre: extractPredicates(operatorType, logicalBinaryExpression->getLeft()))
                predicates.push_back(pre);
            for (auto pre: extractPredicates(operatorType, logicalBinaryExpression->getRight()))
                predicates.push_back(pre);
        }
        predicates.push_back(expression);
        return predicates;
    }

};


#endif //FRONTEND_EXPRESSIONUTILS_HPP
