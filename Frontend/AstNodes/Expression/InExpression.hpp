//
// Created by zxk on 6/29/23.
//

#ifndef OLVP_INEXPRESSION_HPP
#define OLVP_INEXPRESSION_HPP



#include "Expression.h"

class InExpression:public Expression
{
    string inputType;
    vector<string> in_constants;
    Node *condition;

public:
    InExpression(string location,string inputType,vector<string> in_constants): Expression(location,"InExpression"){
        this->inputType = inputType;
        this->in_constants = in_constants;
    }

    Node *getCondition(){return this->condition;}

    string getInputType()
    {
        return this->inputType;
    }
    vector<string> getInConstants()
    {
        return this->in_constants;
    }



    vector<Node*> getChildren(){

        vector<Node*> nodes;
        nodes.push_back(this->condition);
        return nodes;
    }
    void addChilds(vector<Node*> childs) override
    {
        this->condition = childs[0];
    }

    void* accept(AstNodeVisitor *visitor,void* context) {return visitor->VisitInExpression(this,context);}


};




#endif //OLVP_INEXPRESSION_HPP
