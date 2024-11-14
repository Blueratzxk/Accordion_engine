//
// Created by zxk on 10/22/24.
//

#ifndef FRONTEND_SYMBOLREFERENCE_HPP
#define FRONTEND_SYMBOLREFERENCE_HPP

#include "Expression.h"

class SymbolReference : public Expression
{
    string name;

public:
    SymbolReference(string location, string name): Expression(location,"SymbolReference")
    {
        this->name = name;
    }
    string getName()
    {
        return name;
    }
    bool equals(Expression *node) override
    {
        if(node->getExpressionId() != "SymbolReference")
            return false;
        return (this->name == ((SymbolReference*)node)->name);
    }



    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitSymbolReference(this,context);
    }


};
#endif //FRONTEND_SYMBOLREFERENCE_HPP
