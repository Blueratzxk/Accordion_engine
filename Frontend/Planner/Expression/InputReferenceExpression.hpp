//
// Created by zxk on 10/30/24.
//

#ifndef FRONTEND_INPUTREFERENCEEXPRESSION_HPP
#define FRONTEND_INPUTREFERENCEEXPRESSION_HPP

#include "RowExpression.hpp"
#include "../../Analyzer/Type.hpp"

class InputReferenceExpression : public RowExpression {

    int field;
    shared_ptr<Type> type;
    string location;
public:
    InputReferenceExpression(string location,int field, string  type) : RowExpression(location,"InputReferenceExpression")
    {
        this->location = location;
        this->field = field;
        this->type = make_shared<Type>(type);
    }

    shared_ptr<Type> getType()
    {
        return this->type;
    }

    int getValue()
    {
        return this->field;
    }

    std::string getNameOrValue() override
    {
        return to_string(this->field);
    }

    void * accept(RowExpressionVisitor *visitor, void *context) override
    {
        return visitor->VisitInputReference(this,context);
    }

};

#endif //FRONTEND_INPUTREFERENCEEXPRESSION_HPP
