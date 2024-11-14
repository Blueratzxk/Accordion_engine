//
// Created by zxk on 10/22/24.
//

#ifndef FRONTEND_CAST_HPP
#define FRONTEND_CAST_HPP
#include "Expression.h"
class Cast:public Expression
{
    string type;
    Expression *expression;

public:
    Cast(string location, Expression *expression, string type): Expression(location,"Cast"){
        this->expression = expression;
        this->type = type;
    }

    string getType()
    {
        return type;
    }
    Expression *getExpression()
    {
        return this->expression;
    }

    vector<Node*> getChildren() {return {this->expression};}

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitCast(this,context);
    }

};
#endif //FRONTEND_CAST_HPP
