//
// Created by zxk on 11/11/24.
//

#ifndef OLVP_VARIABLEEXTRACTOR_HPP
#define OLVP_VARIABLEEXTRACTOR_HPP

#include "../AstNodes/DefaultAstExpressionVisitor.hpp"
class VariableExtractor
{

public:
    list<string> extractNames(Expression *expression)
    {
        list<string> names;
        NameBuilderVisitor visitor;
        visitor.Visit(expression,&names);
        return names;
    }

    class NameBuilderVisitor : public DefaultAstExpressionVisitor
    {

    public:
        void * VisitIdentifier(Identifier *node, void *context) override
        {
            ((list<string> *)context)->push_back(node->getValue());
            return NULL;
        }
    };


};
#endif //OLVP_VARIABLEEXTRACTOR_HPP
