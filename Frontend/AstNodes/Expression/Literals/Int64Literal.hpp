//
// Created by zxk on 5/18/23.
//

#ifndef OLVP_INT64LITERAL_HPP
#define OLVP_INT64LITERAL_HPP

#include "Literal.h"
class Int64Literal:public Literal
{
    int64_t Value;
public:
    Int64Literal(string location,string value): Literal(location,"Int64Literal")
    {
        this->Value = stol(value);
    }

    int64_t getValue()
    {
        return this->Value;
    }

    bool equals(Expression *node) override
    {
        return (node->getExpressionId() == "Literal" &&
                ((Literal *) node)->getLiteralId() == "Int64Literal" &&
                this->Value == ((Int64Literal *) node)->Value);
    }


    void* accept(AstNodeVisitor *visitor,void* context) {return visitor->VisitInt64Literal(this,context);}

    Literal * createNewOne() override{
        return new DoubleLiteral(this->getLocation(), to_string(this->getValue()));
    }
};

#endif //OLVP_INT64LITERAL_HPP
