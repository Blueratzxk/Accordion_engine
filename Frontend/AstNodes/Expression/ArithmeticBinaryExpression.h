//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_ARITHMETICBINARYEXPRESSION_H
#define FRONTEND_ARITHMETICBINARYEXPRESSION_H

#include "Expression.h"

class ArithmeticBinaryExpression : public Expression
{
public :
    enum Operator {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MODULUS
};
private:

    map<string,Operator> mp = {
            {"+",Operator::ADD},
            {"-",Operator::SUBTRACT},
            {"*",Operator::MULTIPLY},
            {"/",Operator::DIVIDE},
            {"%",Operator::MODULUS}
    };

    map<Operator,string> names = {
            {Operator::ADD,"ADD"},
            {Operator::SUBTRACT,"SUBTRACT"},
            {Operator::MULTIPLY,"MULTIPLY"},
            {Operator::DIVIDE,"DIVIDE"},
            {Operator::MODULUS,"MODULUS"}
    };

    Operator op;
    Expression *left;
    Expression *right;

public:
    ArithmeticBinaryExpression(string location,string opIn,Expression *left,Expression *right) : Expression(location,"ArithmeticBinaryExpression")
    {
        this->op = mp[opIn];
        this->left = left;
        this->right = right;
    }

    static bool isArithmetic(string op)
    {
        if(op == "+" || op == "-" || op == "*" || op == "/" || op == "%")
            return true;
        return false;
    }

    string getOp()
    {

        for(auto m : this->mp)
        {
            if(m.second == this->op)
                return m.first;
        }
        return "Unknown Arith operator!";
    }

    Operator getOperator()
    {
        return this->op;
    }

    string getOPName(Operator op)
    {

        for(auto m : this->names)
        {
            if(m.first == op)
                return m.second;
        }
        return "Unknown Arith operator!";
    }
    vector<Node *> getChildren() override
    {
        return {left,right};
    }


    Expression *getLeft(){return this->left;}
    Expression *getRight(){return this->right;}

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitArithmeticBinaryExpression(this,context);
    }





};
#endif //FRONTEND_ARITHMETICBINARYEXPRESSION_H
