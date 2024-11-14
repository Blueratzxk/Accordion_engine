//
// Created by zxk on 5/18/23.
//

#ifndef OLVP_DOUBLELITERAL_HPP
#define OLVP_DOUBLELITERAL_HPP

#include "Literal.h"
class DoubleLiteral:public Literal
{
    double Value;
public:
    DoubleLiteral(string location,string value): Literal(location,"DoubleLiteral")
    {
        this->Value = stod(value);
    }

    double getValue()
    {
        return this->Value;
    }

    bool equals(Expression *node) override
    {
        return (node->getExpressionId() == "Literal" &&
                ((Literal *) node)->getLiteralId() == "DoubleLiteral" &&
                this->Value == ((DoubleLiteral *) node)->Value);
    }

    void* accept(AstNodeVisitor *visitor,void* context){return visitor->VisitDoubleLiteral(this,context);}

    Literal * createNewOne() override{
        return new DoubleLiteral(this->getLocation(), to_string(this->getValue()));
    }
};


#endif //OLVP_DOUBLELITERAL_HPP
