//
// Created by zxk on 10/30/24.
//

#ifndef FRONTEND_CONSTANTEXPRESSION_HPP
#define FRONTEND_CONSTANTEXPRESSION_HPP

#include "RowExpression.hpp"
#include "../../Analyzer/Type.hpp"

class ConstantExpression : public RowExpression{

    string value;
    shared_ptr<Type> type;

public:
    ConstantExpression(string location, string value,  string type) : RowExpression(location,"ConstantExpression")
    {
        this->value = value;
        this->type = make_shared<Type>(type);
    }

    string getValue()
    {
        return this->value;
    }

    shared_ptr<Type> getType()
    {
        return this->type;
    }

    std::string getNameOrValue() override
    {
        return this->value;
    }

    void * accept(RowExpressionVisitor *visitor, void *context) override
    {
        return visitor->VisitConstant(this,context);
    }

};


#endif //FRONTEND_CONSTANTEXPRESSION_HPP
