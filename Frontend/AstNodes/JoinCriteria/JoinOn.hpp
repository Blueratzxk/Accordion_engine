//
// Created by zxk on 11/3/24.
//

#ifndef FRONTEND_JOINON_HPP
#define FRONTEND_JOINON_HPP
#include "../Expression/Expression.h"
#include "JoinCriteria.hpp"
class JoinOn : public JoinCriteria
{
    Expression *expression;
public:
    JoinOn(Expression *expression): JoinCriteria("JoinOn")
    {
        this->expression = expression;
    }

    Expression *getExpression()
    {
        return this->expression;
    }

};
#endif //FRONTEND_JOINON_HPP
