//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_IDENTIFIER_H
#define FRONTEND_IDENTIFIER_H
#include "Expression.h"
class Identifier:public Expression
{
    string value;
    bool hasDelimiter = false;
public:
    Identifier(string location, string value, bool hasDelimiter = false): Expression(location,"Identifier"){
        this->value = value;
        this->hasDelimiter = hasDelimiter;


    }

    ~Identifier()
    {

    }

    string getValue()
    {
        return value;
    }


    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitIdentifier(this,context);
    }

};

#endif //FRONTEND_IDENTIFIER_H
