//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_LITERAL_H
#define FRONTEND_LITERAL_H

#include "../Expression.h"

class Literal : public Expression
{
public:
    string LiteralId;
public:
    Literal(string location,string literalId): Expression(location,"Literal"){
        this->LiteralId = literalId;
    }


    vector<Node*> getChildren(){
        return {};
    }
    string getLiteralId()
    {
        return this->LiteralId;
    }
    virtual void* accept(AstNodeVisitor *visitor,void* context) {return NULL;}

    virtual Literal* createNewOne() {return NULL;}

};


#endif //FRONTEND_LITERAL_H
