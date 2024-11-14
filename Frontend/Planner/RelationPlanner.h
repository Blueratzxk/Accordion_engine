//
// Created by zxk on 10/24/24.
//

#ifndef FRONTEND_RELATIONPLANNER_H
#define FRONTEND_RELATIONPLANNER_H


#include "../AstNodes/DefaultAstNodeVisitor.hpp"
#include "../Analyzer/Analysis.hpp"
#include "VariableAllocator.hpp"
#include "../AstNodes/AstNodeAllocator.hpp"
#include "../PlanNode/PlanNodeAllocator.hpp"
#include "../Planner/Expression/RowExpressionNodeAllocator.hpp"
#include "QueryPlanner.h"
#include "PlanNodeIdAllocator.hpp"
#include "../Analyzer/ExceptionCollector.hpp"
#include "../Analyzer/FunctionAndTypeResolver.hpp"
#include "Expression/SqlToRowExpressionTranslator.hpp"


class RelationPlanner : public DefaultAstNodeVisitor {

    shared_ptr<Analysis> analysis;
    shared_ptr<VariableAllocator> variableAllocator;
    shared_ptr<PlanNodeIdAllocator> idAllocator;
    shared_ptr<ExceptionCollector> exceptionCollector;
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
    shared_ptr<AstNodeAllocator> astAllocator;
    shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator;
    shared_ptr<PlanNodeAllocator> planNodeAllocator;

public:

    RelationPlanner(shared_ptr<Analysis> analysis,shared_ptr<PlanNodeAllocator> planNodeAllocator);
    RelationPlanner(shared_ptr<Analysis> analysis,shared_ptr<PlanNodeAllocator> planNodeAllocator,shared_ptr<VariableAllocator> variableAllocator,shared_ptr<PlanNodeIdAllocator> idAllocator, shared_ptr<ExceptionCollector> exceptionCollector,
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver,shared_ptr<AstNodeAllocator> astAllocator,shared_ptr<SqlToRowExpressionTranslator> sqlToRowExpressionTranslator) ;

    void releaseAstNodes()
    {
        this->astAllocator->release_all();
     //   this->sqlToRowExpressionTranslator->releaseRowExpressionNodes();
    }

    void *VisitTable(Table *node, void *context) override;

    void * VisitJoin(Join *node, void *context) override;

    void * VisitQuery(Query *node, void *context) override;


    void * VisitQuerySpecification(QuerySpecification *node, void *context) override;

    shared_ptr<PlanBuilder> initializePlanBuilder(RelationPlan *relationPlan);

    shared_ptr<PlanNodeIdAllocator> getPlanNodeIdAllocator(){return this->idAllocator;}

    shared_ptr<ExceptionCollector> getExceptionCollector(){return this->exceptionCollector;}
};

#endif //FRONTEND_RELATIONPLANNER_H
