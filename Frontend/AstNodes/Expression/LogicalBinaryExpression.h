//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_LOGICALBINARYEXPRESSION_H
#define FRONTEND_LOGICALBINARYEXPRESSION_H
#include "Expression.h"




class LogicalBinaryExpression : public Expression
{
public :
    enum Operator {
        AND,OR,UNKNOWN
    };
private:
    Operator op;
    Expression *left;
    Expression *right;



public:
    LogicalBinaryExpression(string location, string op,Expression *left,Expression *right) : Expression(location,"LogicalBinaryExpression")
    {
        if(op == "AND")
            this->op = AND;
        else if(op == "OR")
            this->op = OR;
        else
            this->op = UNKNOWN;

        this->left = left;
        this->right = right;
    }
    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitLogicalBinaryExpression(this,context);
    }


    Expression *getLeft()
    {
        return left;
    }
    Expression *getRight()
    {
        return right;
    }

    string getOp()
    {
        if(op == AND)
            return "AND";
        else if(op == OR)
            return "OR";
        else
            return "Unknown logical operator!";
    }

    Operator getOperatorType()
    {
        return this->op;
    }
    vector<Node *> getChildren() override
    {
        return {left,right};
    }


};


#endif //FRONTEND_LOGICALBINARYEXPRESSION_H
