//
// Created by zxk on 10/21/24.
//

#ifndef FRONTEND_VARIABLEREFERENCEEXPRESSION_HPP
#define FRONTEND_VARIABLEREFERENCEEXPRESSION_HPP


#include "../../Analyzer/Type.hpp"
#include <memory>
#include "RowExpression.hpp"

class VariableReferenceExpression : public RowExpression
{

    string name;
    shared_ptr<Type> type;

public:

    VariableReferenceExpression(string location, string name,string type) : RowExpression(location,"VariableReferenceExpression")
    {
        this->name = name;
        this->type = make_shared<Type>(type);
    }

    string getName()
    {
        return this->name;
    }

    shared_ptr<Type> getType() override{
        return this->type;
    }

    std::string getNameOrValue() override
    {
        return this->name;
    }
    void * accept(RowExpressionVisitor *visitor, void *context) override
    {
        return visitor->VisitVariableReference(this,context);
    }





};
#endif //FRONTEND_VARIABLEREFERENCEEXPRESSION_HPP
