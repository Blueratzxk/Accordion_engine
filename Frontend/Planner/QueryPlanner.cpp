//
// Created by zxk on 10/24/24.
//

#include "QueryPlanner.h"


#include "Assignments.hpp"
#include "Aggregation.hpp"
#include "../Analyzer/ExpressionAnalyzer.hpp"
#include "Expression/TranslateExpressions.hpp"

QueryPlanner::QueryPlanner(shared_ptr<Analysis> analysis, shared_ptr<PlanNodeAllocator> planNodeAllocator,shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator,
                           shared_ptr<ExceptionCollector> exceptionCollector,shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,
                           shared_ptr<AstNodeAllocator> astAllocator,shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator) {

    this->analysis = analysis;
    this->variableAllocator = variableAllocator;
    this->idAllocator = idAllocator;
    this->sqlToRowExpressionTranslator = sqlToRowExpressionTranslator;
    this->exceptionCollector = exceptionCollector;
    this->functionAndTypeResolver = functionAndTypeResolver;
    this->astAllocator = astAllocator;
    this->planNodeAllocator = planNodeAllocator;
}


RelationPlan* QueryPlanner::plan(Query *query)
{
    shared_ptr<PlanBuilder> builder = planQueryBody(query);

    return new RelationPlan(builder->getRoot(), analysis->getScope(query),builder->getRelationPlan()->getFieldMappings());
}

RelationPlan* QueryPlanner::plan(QuerySpecification *node)
{
    shared_ptr<PlanBuilder> builder;
    builder = planFrom(node);
    builder = filter(builder,analysis->getWhere(node),node);
    builder = aggregate(builder,node);

    list<Expression*> outputs = analysis->getOutputExpressions(node);

    builder = project(builder,outputs);

    return new RelationPlan(builder->getRoot(), analysis->getScope(node), computeOutputs(builder,outputs));
}

vector<shared_ptr<VariableReferenceExpression>> QueryPlanner::computeOutputs(shared_ptr<PlanBuilder> builder, list<Expression*> outputExpressions)
{
    vector<shared_ptr<VariableReferenceExpression>> outputs;
    for (auto expression : outputExpressions) {
        outputs.push_back(builder->translate(expression));
    }
    return outputs;
}

shared_ptr<PlanBuilder> QueryPlanner::filter(shared_ptr<PlanBuilder> subPlan, Expression* predicate, Node *node)
{
    if (predicate == NULL) {
        return subPlan;
    }

    // rewrite expressions which contain already handled subqueries
    Expression *rewrittenBeforeSubqueries = subPlan->rewrite(predicate);


    return subPlan->withNewRoot(this->planNodeAllocator->new_FilterNode(node->getLocation(), idAllocator->getNextId(), subPlan->getRoot(), rowExpression(rewrittenBeforeSubqueries)));
}


shared_ptr<PlanBuilder> QueryPlanner::aggregate(shared_ptr<PlanBuilder> subPlan, QuerySpecification *node)
{
    if(!this->analysis->hasAggregation(node))
        return subPlan;

    vector<Expression*> groupByExpressions;
    vector<Expression*> arguments;

    for(auto agg : this->analysis->getAggregates(node))
        for(auto argument : agg->getChildren())
            arguments.push_back((Expression*)argument);

    auto groupBys = this->analysis->getGroupByExpressions(node);
    for(auto groupBy : groupBys)
        groupByExpressions.push_back(groupBy);



    //pre-project
    vector<Expression*> inputs;
    for(auto argument : arguments)
        inputs.push_back(argument);
    for(auto groupBy : groupByExpressions)
        inputs.push_back(groupBy);

    if(!inputs.empty())
        subPlan = project(subPlan,inputs);
    //pre-project



    shared_ptr<TranslationMap> argumentsTranslations = make_shared<TranslationMap>(subPlan->getRelationPlan(),analysis,this->astAllocator,lambdaDeclarationToVariableMap);
    vector<shared_ptr<VariableReferenceExpression>> aggregationArguments;

    for(auto argument : arguments)
    {
        shared_ptr<VariableReferenceExpression> variable = subPlan->translate(argument);
        argumentsTranslations->put(argument,variable);
        aggregationArguments.push_back(variable);
    }

    map<shared_ptr<VariableReferenceExpression>,shared_ptr<VariableReferenceExpression>> groupingSetMappings;

    shared_ptr<TranslationMap> groupingTranslations = make_shared<TranslationMap>(subPlan->getRelationPlan(),analysis,this->astAllocator,lambdaDeclarationToVariableMap);

    for(auto expression : groupByExpressions)
    {
        shared_ptr<VariableReferenceExpression> input = subPlan->translate(expression);
        shared_ptr<VariableReferenceExpression> output = newVariable(variableAllocator,expression, make_shared<Type>(this->analysis->getTypeWithCoercions(expression)->getType()),"gid");
        groupingTranslations->put(expression,output);
        groupingSetMappings[output] = input;
    }

    shared_ptr<AssignmentsBuilder> assignmentsBuilder = make_shared<AssignmentsBuilder>();
    for(auto aggArgument : aggregationArguments)
        assignmentsBuilder->put(aggArgument,aggArgument);
    for(auto grouping : groupingSetMappings)
        assignmentsBuilder->put(grouping.first,grouping.second);
    shared_ptr<Assignments> assignments = assignmentsBuilder->build();
    ProjectNode *project = this->planNodeAllocator->new_ProjectNode(idAllocator->getNextId(),subPlan->getRoot(),assignments);
    subPlan = make_shared<PlanBuilder>(groupingTranslations,project);





    shared_ptr<TranslationMap> aggregationTranslations = make_shared<TranslationMap>(subPlan->getRelationPlan(),analysis,this->astAllocator,lambdaDeclarationToVariableMap);
    aggregationTranslations->copyMappingsFrom(groupingTranslations);




    map<shared_ptr<VariableReferenceExpression>,shared_ptr<Aggregation>> aggregations;
    for(auto aggregate : analysis->getAggregates(node))
    {
        Expression *rewritten = argumentsTranslations->rewrite(aggregate);
        shared_ptr<VariableReferenceExpression> newVar = newVariable(variableAllocator, rewritten, make_shared<Type>(analysis->getType(aggregate)->getType()),"");
        aggregationTranslations->put(aggregate,newVar);

        shared_ptr<FunctionHandle> functionHandle = analysis->getFunctionHandle(aggregate);


        list<shared_ptr<RowExpression>> arguments;

        for(auto argument : rewritten->getChildren()) {
            auto rowExpression = this->rowExpression((Expression *) argument);
            arguments.push_back(rowExpression);

        }

        auto call = make_shared<CallExpression>(rewritten->getLocation(),aggregate->getFuncName(),functionHandle, make_shared<Type>(analysis->getType(aggregate)->getType()),arguments);
        auto aggregation = make_shared<Aggregation>(call);
        aggregations[newVar] = aggregation;

    }

    list<shared_ptr<VariableReferenceExpression>> groupByVariables;
    auto groupByExps = this->analysis->getGroupByExpressions(node);
    for(auto ge : groupByExpressions)
        groupByVariables.push_back(subPlan->translate(ge));


    AggregationNode *aggregationNode = this->planNodeAllocator->new_AggregationNode(idAllocator->getNextId(),subPlan->getRoot(),aggregations,groupByVariables);

    subPlan = make_shared<PlanBuilder>(aggregationTranslations, aggregationNode);

    return  subPlan;

}




shared_ptr<VariableReferenceExpression>
QueryPlanner::newVariable(shared_ptr<VariableAllocator> variableAllocator, Expression *expression,
                          shared_ptr<Type> type, string suffix) {

    return variableAllocator->newVariable(expression->getLocation(), variableAllocator->getNameHint(expression), type, suffix);

}


shared_ptr<PlanBuilder> QueryPlanner::planFrom(QuerySpecification *node)
{
    RelationPlan *relationPlan;

    if (node->getFrom() != NULL) {
        relationPlan = (RelationPlan*)make_shared<RelationPlanner>(this->analysis,this->planNodeAllocator,this->variableAllocator,
                                                                   this->idAllocator,this->exceptionCollector,
                                                                   this->functionAndTypeResolver,this->astAllocator,
                                                                   this->sqlToRowExpressionTranslator)->Visit(node->getFrom(),NULL);
    }

    return planBuilderFor(relationPlan);
}

shared_ptr<PlanBuilder> QueryPlanner::planQueryBody(Query *query)
{
    RelationPlan *relationPlan = (RelationPlan *)make_shared<RelationPlanner>(this->analysis,this->planNodeAllocator,this->variableAllocator,
                                                                              this->idAllocator,this->exceptionCollector,
                                                                              this->functionAndTypeResolver,this->astAllocator,
                                                                              this->sqlToRowExpressionTranslator)->Visit(query->getQueryBody(), NULL);

    return planBuilderFor(relationPlan);
}

shared_ptr<PlanBuilder> QueryPlanner::planBuilderFor(RelationPlan *relationPlan)
{

    shared_ptr<RelationPlan> relationPlanPtr = shared_ptr<RelationPlan>(relationPlan);

    vector<shared_ptr<VariableReferenceExpression>> fieldvariables;

    shared_ptr<TranslationMap> translations = make_shared<TranslationMap>(relationPlanPtr, analysis,this->astAllocator,fieldvariables);

    translations->setFieldMappings(relationPlan->getFieldMappings());

    return make_shared<PlanBuilder>(translations, relationPlan->getRoot());
}

shared_ptr<PlanBuilder> QueryPlanner::planBuilderFor(shared_ptr<PlanBuilder> builder, Scope *scope)
{
    return planBuilderFor(new RelationPlan(builder->getRoot(), scope, builder->getRoot()->getOutputVariables()));
}



shared_ptr<RowExpression>rowExpression(Expression *expression,shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,shared_ptr<ExceptionCollector> exceptionCollector,
                                       shared_ptr<AstNodeAllocator> astAllocator,shared_ptr<VariableAllocator> variableAllocator,
                                       shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator) {

    shared_ptr<Analysis> analysis = make_shared<Analysis>("");
    shared_ptr<ExpressionAnalyzer> expressionAnalyzer = make_shared<ExpressionAnalyzer>(analysis,functionAndTypeResolver,exceptionCollector,astAllocator);
    auto result = expressionAnalyzer->analyzeExpression(variableAllocator->getTypes(),expression);

    auto symbolTypes = result->getTypes();
    auto re = sqlToRowExpressionTranslator->translate(expression,functionAndTypeResolver,symbolTypes);

    return re;

}


shared_ptr<RowExpression> QueryPlanner::rowExpression(Expression *expression) {

   auto re =  TranslateExpressions::rowExpression(expression,this->functionAndTypeResolver,exceptionCollector,astAllocator,variableAllocator,sqlToRowExpressionTranslator);
   return re;

}


shared_ptr<VariableReferenceExpression> QueryPlanner::toVariableReference(shared_ptr<VariableAllocator> variableAllocator, Expression *expression)
{
    string name = ((SymbolReference*) expression)->getName();
    return variableAllocator->getVariableReferenceExpression(expression->getLocation(), name);
}

list<Expression *> QueryPlanner::toSymbolReferences(vector<shared_ptr<VariableReferenceExpression>> variables) {
    list<Expression *> symbols;
    for(auto variable : variables)
    {
        symbols.push_back(this->astAllocator->new_SymbolReference(variable->getSourceLocation(),variable->getName()));
    }
    return symbols;
}



shared_ptr<PlanBuilder> QueryPlanner::project(shared_ptr<PlanBuilder> subPlan, vector<Expression*> expressions, RelationPlan *parentRelationPlan)
{
    vector<Expression *> allExpressions;

    for(auto expression : expressions)
        allExpressions.push_back(expression);

    for(auto expression : toSymbolReferences(parentRelationPlan->getFieldMappings()))
        allExpressions.push_back(expression);


    return project(subPlan, allExpressions);
}

shared_ptr<PlanBuilder> QueryPlanner::project(shared_ptr<PlanBuilder> subPlan, list<Expression*> expressions)
{
    vector<Expression *> allExpressions;

    for(auto expression : expressions)
        allExpressions.push_back(expression);


    return project(subPlan, allExpressions);
}

shared_ptr<PlanBuilder> QueryPlanner::project(shared_ptr<PlanBuilder> subPlan, vector<Expression *> expressions) {
    shared_ptr<TranslationMap> outputTranslations = make_shared<TranslationMap>(subPlan->getRelationPlan(), analysis, this->astAllocator,lambdaDeclarationToVariableMap);

    shared_ptr<AssignmentsBuilder> projections = make_shared<AssignmentsBuilder>();
    for (Expression *expression : expressions) {
        if (expression->getExpressionId() == "SymbolReference") {
            shared_ptr<VariableReferenceExpression> variable = toVariableReference(variableAllocator, expression);
            projections->put(variable, rowExpression(expression));
            outputTranslations->put(expression, variable);
            continue;
        }

        shared_ptr<VariableReferenceExpression> variable = newVariable(variableAllocator, expression, make_shared<Type>(analysis->getType(expression)->getType()),"");
        auto rewrittenExpression = subPlan->rewrite(expression);
        auto toRowExpression = rowExpression(rewrittenExpression);
        projections->put(variable, toRowExpression);
        outputTranslations->put(expression, variable);
    }

    return make_shared<PlanBuilder>(outputTranslations, this->planNodeAllocator->new_ProjectNode(
            idAllocator->getNextId(),
            subPlan->getRoot(),
            projections->build()));
}





