//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_NOTEXPRESSION_H
#define FRONTEND_NOTEXPRESSION_H



#include "Expression.h"
class NotExpression : public Expression
{
    Expression *expression;


public:
    NotExpression(string location,Expression *expression) : Expression(location,"NotExpression")
    {
        this->expression = expression;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitNotExpression(this,context);
    }
    string getOp()
    {
        return "NOT";
    }

    Expression *getExpression()
    {
        return this->expression;
    }


    vector<Node *> getChildren() override
    {
        return {expression};
    }

};


#endif //FRONTEND_NOTEXPRESSION_H
