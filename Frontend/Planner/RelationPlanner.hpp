//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_RELATIONPLANNER_HPP
#define FRONTEND_RELATIONPLANNER_HPP
/*
#include "../AstNodes/DefaultAstNodeVisitor.hpp"
#include "../Analyzer/Analysis.hpp"
#include "VariableAllocator.hpp"
#include "QueryPlanner.hpp"
class RelationPlanner : public DefaultAstNodeVisitor {

shared_ptr<Analysis> analysis;
shared_ptr<VariableAllocator> variableAllocator;

public:

    RelationPlanner(shared_ptr<Analysis> analysis,shared_ptr<VariableAllocator> variableAllocator) {
        this->analysis = analysis;
        this->variableAllocator = variableAllocator;
    }

    void *VisitTable(Table *node, void *context) override
    {

    }

    void * VisitJoin(Join *node, void *context) override
    {

        Visit(node->getLeft(),context);
        Visit(node->getRight(),context);

    }

    void * VisitQuery(Query *node, void *context) override
    {
        return (new QueryPlanner(analysis, variableAllocator))->plan(node);
    }


    void * VisitQuerySpecification(QuerySpecification *node, void *context) override
    {
        return (new QueryPlanner(analysis, variableAllocator))->plan(node);
    }
};


*/


#endif //FRONTEND_RELATIONPLANNER_HPP
