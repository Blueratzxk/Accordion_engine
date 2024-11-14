//
// Created by zxk on 11/3/24.
//

#ifndef FRONTEND_NATURALJOIN_HPP
#define FRONTEND_NATURALJOIN_HPP
#include "../Expression/Expression.h"
#include "JoinCriteria.hpp"
class NaturalJoin : public JoinCriteria
{
    Expression *expression;
public:
    NaturalJoin(Expression *expression): JoinCriteria("NaturalJoin")
    {
        this->expression = expression;
    }

    Expression *getExpression()
    {
        return this->expression;
    }

};
#endif //FRONTEND_NATURALJOIN_HPP
