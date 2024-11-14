//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_EXPRESSION_H
#define FRONTEND_EXPRESSION_H

#include "../Node.h"
class Expression:public Node
{
    string ExpressionId;
public:
    Expression(string location,string ExpressionId): Node(location,"Expression"){
        this->ExpressionId = ExpressionId;
    }
    string getExpressionId()
    {
        return this->ExpressionId;
    }
    virtual void* accept(AstNodeVisitor *visitor,void* context) {return NULL;}

    virtual  void addChilds(vector<Expression*> childs){}

    virtual bool equals(Expression *node){return false;}

    ~Expression() override = default;

};


#endif //FRONTEND_EXPRESSION_H
