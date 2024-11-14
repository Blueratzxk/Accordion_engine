//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_QUERYPLANNER_HPP
#define FRONTEND_QUERYPLANNER_HPP

/*
#include "../Analyzer/Analysis.hpp"
#include "VariableAllocator.hpp"
#include "RelationPlan.hpp"
#include "PlanBuilder.hpp"
#include "RelationPlanner.hpp"
class QueryPlanner
{
    shared_ptr<Analysis> analysis;
    shared_ptr<VariableAllocator> variableAllocator;
public:
    QueryPlanner(shared_ptr<Analysis> analysis, shared_ptr<VariableAllocator> variableAllocator)
    {
        this->analysis = analysis;
        this->variableAllocator = variableAllocator;
    }

    RelationPlan* plan(Query *query)
    {
        shared_ptr<PlanBuilder> builder = planQueryBody(query);
        return new RelationPlan(builder->getRoot(), NULL,{});
    }

    RelationPlan* plan(QuerySpecification *node)
    {
        shared_ptr<PlanBuilder> builder = planFrom(node);
        RelationPlan *fromRelationPlan = builder->getRelationPlan().get();

        return new RelationPlan(builder->getRoot(), NULL,{});
    }

    shared_ptr<PlanBuilder> planFrom(QuerySpecification *node)
    {
        RelationPlan *relationPlan;

        if (node->getFrom() != NULL) {
            relationPlan = (RelationPlan*)make_shared<RelationPlanner>(this->analysis,this->variableAllocator)->Visit(node,NULL);
        }

        return planBuilderFor(relationPlan);
    }

    shared_ptr<PlanBuilder> planQueryBody(Query *query)
    {
        RelationPlan *relationPlan = (RelationPlan *)make_shared<RelationPlanner>(this->analysis,this->variableAllocator)->Visit(query->getQueryBody(), NULL);

        return planBuilderFor(relationPlan);
    }

    shared_ptr<PlanBuilder> planBuilderFor(RelationPlan *relationPlan)
    {

        vector<shared_ptr<VariableReferenceExpression>> fieldvariables;
        shared_ptr<TranslationMap> translations = make_shared<TranslationMap>(shared_ptr<RelationPlan>(relationPlan), analysis, fieldvariables);

        translations->setFieldMappings(relationPlan->getFieldMappings());

        return make_shared<PlanBuilder>(translations, relationPlan->getRoot());
    }

    shared_ptr<PlanBuilder> planBuilderFor(shared_ptr<PlanBuilder> builder, Scope *scope)
    {
        return planBuilderFor(new RelationPlan(builder->getRoot(), scope, builder->getRoot()->getOutputVariables()));
    }

};
*/
#endif //FRONTEND_QUERYPLANNER_HPP
