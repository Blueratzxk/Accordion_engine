//
// Created by zxk on 10/30/24.
//

#ifndef FRONTEND_ORIGINROWEXPRESSION_HPP
#define FRONTEND_ORIGINROWEXPRESSION_HPP

#include "RowExpression.hpp"
#include "../../AstNodes/Expression/Expression.h"

class OriginRowExpression : RowExpression {

    Expression *expression;
public:
    OriginRowExpression(string location,Expression *expression) : RowExpression(location,"OriginRowExpression")
    {
        this->expression = expression;
    }

    shared_ptr<Type> getType()
    {
        return NULL;
    }
};


#endif //FRONTEND_ORIGINROWEXPRESSION_HPP
