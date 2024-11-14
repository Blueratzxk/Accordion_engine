//
// Created by zxk on 10/24/24.
//

#ifndef FRONTEND_QUERYPLANNER_H
#define FRONTEND_QUERYPLANNER_H


#include "../Analyzer/Analysis.hpp"
#include "VariableAllocator.hpp"
#include "RelationPlan.hpp"
#include "../PlanNode/PlanNodeTree.hpp"
#include "PlanBuilder.hpp"
#include "RelationPlanner.h"
#include "PlanNodeIdAllocator.hpp"
#include "../Analyzer/ExceptionCollector.hpp"
#include "../Analyzer/FunctionAndTypeResolver.hpp"


class SqlToRowExpressionTranslator;
class QueryPlanner
{
    shared_ptr<Analysis> analysis;
    shared_ptr<VariableAllocator> variableAllocator;
    shared_ptr<PlanNodeIdAllocator> idAllocator;
    shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator;

    vector<shared_ptr<VariableReferenceExpression>> lambdaDeclarationToVariableMap;

    shared_ptr<ExceptionCollector> exceptionCollector;
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
    shared_ptr<AstNodeAllocator> astAllocator;
    shared_ptr<PlanNodeAllocator> planNodeAllocator;

public:
    QueryPlanner(shared_ptr<Analysis> analysis,shared_ptr<PlanNodeAllocator> planNodeAllocator, shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator, shared_ptr<ExceptionCollector> exceptionCollector,
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,shared_ptr<AstNodeAllocator> astAllocator,shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator);

    RelationPlan* plan(Query *query);

    RelationPlan* plan(QuerySpecification *node);
    shared_ptr<PlanBuilder> planFrom(QuerySpecification *node);
    shared_ptr<PlanBuilder> planQueryBody(Query *query);
    shared_ptr<PlanBuilder> planBuilderFor(RelationPlan *relationPlan);
    shared_ptr<PlanBuilder> planBuilderFor(shared_ptr<PlanBuilder> builder, Scope *scope);
    shared_ptr<PlanBuilder> filter(shared_ptr<PlanBuilder> subPlan, Expression* predicate, Node *node);
    shared_ptr<PlanBuilder> aggregate(shared_ptr<PlanBuilder> subPlan, QuerySpecification *node);
    shared_ptr<VariableReferenceExpression> newVariable(shared_ptr<VariableAllocator> variableAllocator, Expression *expression, shared_ptr<Type> type, string suffix);
    shared_ptr<RowExpression> rowExpression(Expression *expression);
    list<Expression*> toSymbolReferences(vector<shared_ptr<VariableReferenceExpression>> variables);
    shared_ptr<VariableReferenceExpression> toVariableReference(shared_ptr<VariableAllocator> variableAllocator, Expression *expression);
    shared_ptr<PlanBuilder> project(shared_ptr<PlanBuilder> subPlan, vector<Expression*> expressions, RelationPlan *parentRelationPlan);
    shared_ptr<PlanBuilder> project(shared_ptr<PlanBuilder> subPlan, vector<Expression*> expressions);
    shared_ptr<PlanBuilder> project(shared_ptr<PlanBuilder> subPlan, list<Expression*> expressions);
    vector<shared_ptr<VariableReferenceExpression>> computeOutputs(shared_ptr<PlanBuilder> builder, list<Expression*> outputExpressions);
};




#endif //FRONTEND_QUERYPLANNER_H
