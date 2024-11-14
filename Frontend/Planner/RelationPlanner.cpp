//
// Created by zxk on 10/24/24.
//

#include "RelationPlanner.h"
#include "Expression/SqlToRowExpressionTranslator.hpp"
#include "Expression/EquiJoinClause.hpp"
#include "../AstNodes/Expression/ExpressionUtils.hpp"
#include "../AstNodes/JoinCriteria/JoinOn.hpp"
#include "VariableExtractor.hpp"

RelationPlanner::RelationPlanner(shared_ptr<Analysis> analysis,shared_ptr<PlanNodeAllocator> planNodeAllocator,shared_ptr<VariableAllocator> variableAllocator,shared_ptr<PlanNodeIdAllocator> idAllocator,
                                 shared_ptr<ExceptionCollector> exceptionCollector,shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,
                                 shared_ptr<AstNodeAllocator> astAllocator,shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator) {
    this->analysis = analysis;
    this->variableAllocator = variableAllocator;
    this->idAllocator = idAllocator;
    this->exceptionCollector = exceptionCollector;
    this->functionAndTypeResolver = functionAndTypeResolver;
    this->astAllocator = astAllocator;
    this->sqlToRowExpressionTranslator = sqlToRowExpressionTranslator;
    this->planNodeAllocator = planNodeAllocator;
}

void *RelationPlanner::VisitTable(Table *node, void *context)
{


    Scope *scope = this->analysis->getScope(node);

    auto allfields = scope->getRelationType()->getAllFields();
    map<shared_ptr<VariableReferenceExpression>,shared_ptr<ColumnHandle>> columns;

    vector<shared_ptr<VariableReferenceExpression>> outputVariables;
    for(auto field : allfields)
    {
        shared_ptr<VariableReferenceExpression> variable = variableAllocator->newVariable(node->getLocation(),field->getValue(),
                                                                                                 make_shared<Type>(field->getType()));
        outputVariables.push_back(variable);
        columns[variable] = analysis->getColumn(field);
    }

    PlanNode *root = this->planNodeAllocator->new_TableScanNode(node->getLocation(),this->idAllocator->getNextId(),this->analysis->getTableHandle(node),outputVariables,columns);

    auto newRelationPlan = new RelationPlan(root,scope,outputVariables);
    return newRelationPlan;

}

void * RelationPlanner::VisitJoin(Join *node, void *context) {

    RelationPlan *leftPlan = (RelationPlan *) Visit(node->getLeft(), context);
    RelationPlan *rightPlan = (RelationPlan *) Visit(node->getRight(), context);

    auto leftRelationType = this->analysis->getScope(node->getLeft())->getRelationType();
    auto rightRelationType =  this->analysis->getScope(node->getRight())->getRelationType();

    shared_ptr<PlanBuilder> leftPlanBuilder = initializePlanBuilder(leftPlan);
    shared_ptr<PlanBuilder> rightPlanBuilder = initializePlanBuilder(rightPlan);

    vector<shared_ptr<VariableReferenceExpression>> outputs;

    for (const auto &field: leftPlanBuilder->getRelationPlan()->getFieldMappings())
        outputs.push_back(field);
    for (const auto &field: rightPlanBuilder->getRelationPlan()->getFieldMappings())
        outputs.push_back(field);

    list<shared_ptr<EquiJoinClause>> equiJoinClauses;
    list<ComparisionExpression *> joinExpressions;

    Expression *predicates;
    predicates = dynamic_pointer_cast<JoinOn>(node->getJoinCriteria())->getExpression();

    auto conjucts = ExpressionUtils::extractConjuncts(predicates);

    for (auto conjuct: conjucts) {
        if (conjuct->getExpressionId() == "ComparisionExpression" &&
            ((ComparisionExpression *) (conjuct))->getOperator() == ComparisionExpression::EQUAL)
            joinExpressions.push_back((ComparisionExpression *) conjuct);
    }


    vector<Expression *> leftComparisonExpressions;
    vector<Expression *> rightComparisonExpressions;

    VariableExtractor variableExtractor;
    for (auto expression: joinExpressions) {


        auto firstDependencies = variableExtractor.extractNames(expression->getLeft());
        auto secondDependencies = variableExtractor.extractNames(expression->getRight());

        bool firstAllMatch = true;
        bool secondAllMatch = true;
        for(auto dep : firstDependencies)
            if(!leftRelationType->canResolve(dep))
                firstAllMatch = false;
        for(auto dep : secondDependencies)
            if(!rightRelationType->canResolve(dep))
                secondAllMatch = false;

        if(firstAllMatch && secondAllMatch)
        {
            leftComparisonExpressions.push_back(expression->getLeft());
            rightComparisonExpressions.push_back(expression->getRight());
            continue;
        }

        firstAllMatch = true;
        secondAllMatch = true;

        for(auto dep : firstDependencies)
            if(!rightRelationType->canResolve(dep))
                firstAllMatch = false;
        for(auto dep : secondDependencies)
            if(!leftRelationType->canResolve(dep))
                secondAllMatch = false;

        if(firstAllMatch && secondAllMatch)
        {
            leftComparisonExpressions.push_back(expression->getRight());
            rightComparisonExpressions.push_back(expression->getLeft());
        }

    }
    if(leftComparisonExpressions.empty() || rightComparisonExpressions.empty())
        this->exceptionCollector->recordError("Wrong join clause !");

    leftPlanBuilder = leftPlanBuilder->appendProjections(leftComparisonExpressions, planNodeAllocator,variableAllocator, idAllocator, astAllocator,this->exceptionCollector,this->functionAndTypeResolver,this->analysis,this->sqlToRowExpressionTranslator);
    rightPlanBuilder = rightPlanBuilder->appendProjections(rightComparisonExpressions, planNodeAllocator,variableAllocator, idAllocator, astAllocator,this->exceptionCollector,this->functionAndTypeResolver,this->analysis,this->sqlToRowExpressionTranslator);


    for (int i = 0; i < leftComparisonExpressions.size(); i++) {

        shared_ptr<VariableReferenceExpression> leftVariable = leftPlanBuilder->translateToVariable(
                leftComparisonExpressions[i]);
        shared_ptr<VariableReferenceExpression> rightVariable = rightPlanBuilder->translateToVariable(
                rightComparisonExpressions[i]);
        equiJoinClauses.push_back(make_shared<EquiJoinClause>(leftVariable, rightVariable));
    }

    vector<shared_ptr<VariableReferenceExpression>> outputVariables;
    auto leftVariables = leftPlanBuilder->getRoot()->getOutputVariables();
    auto rightVariables = rightPlanBuilder->getRoot()->getOutputVariables();

    for(auto variable : leftVariables)
        outputVariables.push_back(variable);
    for(auto variable : rightVariables)
        outputVariables.push_back(variable);


    PlanNode *root = this->planNodeAllocator->new_LookupJoinNode(
            idAllocator->getNextId(),
            leftPlanBuilder->getRoot(),
            rightPlanBuilder->getRoot(),
            equiJoinClauses,outputs);

    vector<shared_ptr<VariableReferenceExpression>> lambdaDeclarationToVariableMap;
    auto intermediateRootRelationPlan = make_shared<RelationPlan>(root, analysis->getScope(node), outputs);
    shared_ptr<TranslationMap> translationMap = make_shared<TranslationMap>(intermediateRootRelationPlan,analysis,astAllocator,lambdaDeclarationToVariableMap);

    translationMap->setFieldMappings(outputs);
    translationMap->putExpressionMappingsFrom(leftPlanBuilder->getTranslations());
    translationMap->putExpressionMappingsFrom(rightPlanBuilder->getTranslations());


    return new RelationPlan(root, analysis->getScope(node), outputs);
}

shared_ptr<PlanBuilder> RelationPlanner::initializePlanBuilder(RelationPlan *relationPlan)
{
    vector<shared_ptr<VariableReferenceExpression>> lambdaDeclarationToVariableMap;
    shared_ptr<TranslationMap> translations = make_shared<TranslationMap>(shared_ptr<RelationPlan>(relationPlan), analysis,this->astAllocator,lambdaDeclarationToVariableMap);


    translations->setFieldMappings(relationPlan->getFieldMappings());

    return make_shared<PlanBuilder>(translations, relationPlan->getRoot());
}


void * RelationPlanner::VisitQuery(Query *node, void *context) {
    return make_shared<QueryPlanner>(analysis, planNodeAllocator,variableAllocator,idAllocator,exceptionCollector,functionAndTypeResolver,astAllocator,sqlToRowExpressionTranslator)->plan(node);

}


void * RelationPlanner::VisitQuerySpecification(QuerySpecification *node, void *context) {

    auto queryPlanner = make_shared<QueryPlanner>(analysis, planNodeAllocator,variableAllocator,idAllocator,exceptionCollector,functionAndTypeResolver,astAllocator,sqlToRowExpressionTranslator);
    auto result = queryPlanner->plan(node);

    return result;
}

RelationPlanner::RelationPlanner(shared_ptr<Analysis> analysis,shared_ptr<PlanNodeAllocator> planNodeAllocator)
{
    this->analysis = analysis;
    this->variableAllocator = make_shared<VariableAllocator>();
    this->idAllocator = make_shared<PlanNodeIdAllocator>();
    this->exceptionCollector = make_shared<ExceptionCollector>();
    this->functionAndTypeResolver = make_shared<FunctionAndTypeResolver>(this->exceptionCollector);
    this->astAllocator = make_shared<AstNodeAllocator>();
    this->sqlToRowExpressionTranslator = make_shared<SqlToRowExpressionTranslator>();
    this->planNodeAllocator = planNodeAllocator;
}
