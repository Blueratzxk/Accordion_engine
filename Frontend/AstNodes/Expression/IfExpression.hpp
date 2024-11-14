//
// Created by zxk on 6/27/23.
//

#ifndef OLVP_IFEXPRESSION_HPP
#define OLVP_IFEXPRESSION_HPP


#include "Expression.h"

class IfExpression:public Expression
{
    string outputType;
    Node *condition;
    Node *thenAction;
    Node *elseAction;

public:
    IfExpression(string location,string outputType): Expression(location,"IfExpression"){
        this->outputType = outputType;
    }

    Node *getCondition(){return this->condition;}

    Node *getThenAction(){return this->thenAction;}

    Node *getElseAction(){return this->elseAction;}


    void setIfBody(Node *conditionIn,Node *thenActionIn,Node *elseActionIn)
    {
        this->condition = conditionIn;
        this->thenAction = thenActionIn;
        this->elseAction = elseActionIn;
    }


    string getOutputType()
    {
        return this->outputType;
    }


    vector<Node*> getChildren(){

        vector<Node*> nodes;
        nodes.push_back(this->condition);
        nodes.push_back(this->thenAction);
        nodes.push_back(this->elseAction);
        return nodes;
    }
    void addChilds(vector<Node*> childs) override
    {
        this->condition = childs[0];
        this->thenAction = childs[1];
        this->elseAction = childs[2];
    }

    void* accept(AstNodeVisitor *visitor,void* context) {return visitor->VisitIfExpression(this,context);}


};



#endif //OLVP_IFEXPRESSION_HPP
