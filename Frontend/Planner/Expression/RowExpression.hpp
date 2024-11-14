//
// Created by zxk on 10/21/24.
//

#ifndef FRONTEND_ROWEXPRESSION_HPP
#define FRONTEND_ROWEXPRESSION_HPP

#include <string>
#include "../../Analyzer/Type.hpp"
#include <memory>

class RowExpressionVisitor;
class RowExpression
{
    std::string location;
    std::string expressionName;
public:
    RowExpression(std::string location,std::string name){
        this->location = location;
        this->expressionName = name;
    }
    virtual std::string getSourceLocation()
    {
        return this->location;
    }

    virtual std::string getExpressionName()
    {
        return this->expressionName;
    }

    virtual std::string getNameOrValue() = 0;

    virtual shared_ptr<Type> getType() = 0;

    virtual void *accept(RowExpressionVisitor *visitor, void *context) { return NULL; };

    virtual ~RowExpression() = default;
};

#endif //FRONTEND_ROWEXPRESSION_HPP
