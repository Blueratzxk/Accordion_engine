//
// Created by zxk on 6/17/23.
//

#ifndef OLVP_DATE32LITERAL_HPP
#define OLVP_DATE32LITERAL_HPP


#include "Literal.h"

class Date32Literal:public Literal
{
    string Value;
public:
    Date32Literal(string location,string value): Literal(location,"Date32Literal")
    {
        this->Value = value;
    }

    string getValue()
    {
        return this->Value;
    }
    bool equals(Expression *node) override
    {
        return (node->getExpressionId() == "Literal" &&
                ((Literal *) node)->getLiteralId() == "Date32Literal" &&
                this->Value == ((Date32Literal *) node)->Value);
    }

    void* accept(AstNodeVisitor *visitor,void* context) {return visitor->VisitDate32Literal(this,context);}

    Literal * createNewOne() override{
        return new Date32Literal(this->getLocation(),this->getValue());
    }
};


#endif //OLVP_DATE32LITERAL_HPP
