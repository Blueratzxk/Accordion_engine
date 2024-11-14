//
// Created by zxk on 11/3/24.
//

#ifndef FRONTEND_JOINUSING_HPP
#define FRONTEND_JOINUSING_HPP
#include "../Expression/Expression.h"
#include "JoinCriteria.hpp"
class JoinUsing : public JoinCriteria
{
    Expression *expression;
public:
    JoinUsing(Expression *expression): JoinCriteria("JoinUsing")
    {
        this->expression = expression;
    }

    Expression *getExpression()
    {
        return this->expression;
    }

};
#endif //FRONTEND_JOINUSING_HPP
