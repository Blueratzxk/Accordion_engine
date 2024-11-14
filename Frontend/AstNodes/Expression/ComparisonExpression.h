//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_COMPARISONEXPRESSION_H
#define FRONTEND_COMPARISONEXPRESSION_H

#include "Expression.h"

class ComparisionExpression : public Expression
{
public :
    enum Operator {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        IS_DISTINCT_FROM
    };
private:

    map<string,Operator> mp = {
            {"=",Operator::EQUAL},
            {"<>",Operator::NOT_EQUAL},
            {"<",Operator::LESS_THAN},
            {"<=",Operator::LESS_THAN_OR_EQUAL},
            {">",Operator::GREATER_THAN},
            {">=",Operator::GREATER_THAN_OR_EQUAL},
            {"IS DISTINCT FROM",Operator::IS_DISTINCT_FROM}
    };

    map<Operator,string> names = {
            {Operator::EQUAL,"EQUAL"},
            {Operator::NOT_EQUAL,"NOT_EQUAL"},
            {Operator::LESS_THAN,"LESS_THAN"},
            {Operator::LESS_THAN_OR_EQUAL,"LESS_THAN_OR_EQUAL"},
            {Operator::GREATER_THAN,"GREATER_THAN"},
            {Operator::GREATER_THAN_OR_EQUAL,"GREATER_THAN_OR_EQUAL"},
            {Operator::IS_DISTINCT_FROM,"IS_DISTINCT_FROM"}
    };

    Operator op;
    Expression *left;
    Expression *right;

public:
    static bool isComparision(string op)
    {
        if(op == "=" || op == "<>" || op == "<" || op == "<=" || op == ">" || op == ">=" || op == "IS DISTINCT FROM")
            return true;
        return false;
    }
    ComparisionExpression(string location,string opIn,Expression *left,Expression *right) : Expression(location,"ComparisionExpression")
    {
        this->op = mp[opIn];
        this->left = left;
        this->right = right;

    }

    string getOp()
    {

        for(auto m : this->mp)
        {
            if(m.second == this->op)
                return m.first;
        }
        return "Unknown compare operator!";

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
        return "Unknown compare operator!";
    }
    vector<Node *> getChildren() override
    {
        return {left,right};
    }

    Expression *getLeft()
    {
        return this->left;
    }
    Expression *getRight()
    {
        return this->right;
    }
    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitComparisionExpression(this,context);
    }






};
#endif //FRONTEND_COMPARISONEXPRESSION_H
