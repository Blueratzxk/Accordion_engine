//
// Created by zxk on 11/12/24.
//

#ifndef OLVP_OPTIMIZEOUTPUTPROJECT_HPP
#define OLVP_OPTIMIZEOUTPUTPROJECT_HPP


#include "PlanOptimizer.h"
#include "SimplePlanNodeRewriter.hpp"
#include "../Analyzer/FunctionAndTypeResolver.hpp"
#include "../Planner/Expression/RowExpressionTreeRewriter.hpp"

class OptimizeOutputProject : public PlanOptimizer
{
    class RewriteContext
    {

        class RowRewriter : public  RowExpressionTreeRewriter::RowExpressionRewriter
        {
        public:

            RowExpression * rewriteVariableReferenceExpression(VariableReferenceExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) override {
                auto rewritecontext = (RewriteContext *) ((RowExpressionTreeRewriter::ExpressionTreeRewriterContext *) context)->get();

                string newName = rewritecontext->getNewName(node->getName());

                auto newVariable = new VariableReferenceExpression(node->getSourceLocation(), newName,
                                                                   node->getType()->getType());
                return newVariable;
            }

            RowExpression * rewriteCall(CallExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) override
            {

                auto rewritecontext = (RewriteContext*)((RowExpressionTreeRewriter::ExpressionTreeRewriterContext*)context)->get();

                auto arguments = node->getArguments();

                list<shared_ptr<RowExpression>> rewrittenResults;
                for(auto argument : arguments)
                {
                    auto re = treeRewriter->rewrite(argument.get(),rewritecontext);
                    rewrittenResults.push_back(shared_ptr<RowExpression>((RowExpression*)re));
                }

                return new CallExpression(node->getSourceLocation(),node->getDisplayName(),node->getFunctionHandle(),node->getReturnType(),rewrittenResults);
            }

        };

        shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter;
        shared_ptr<RowRewriter> rowRewriter;

        map<string,string> rewriteMap;

    public:
        RewriteContext(shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter)
        {
            this->rowExpressionTreeRewriter = rowExpressionTreeRewriter;
            this->rowRewriter = make_shared<RowRewriter>();
        }

        RowExpression* rewrite(RowExpression *node, void *context)
        {
            return (RowExpression*)this->rowExpressionTreeRewriter->rewriteWith(rowRewriter,node,context);
        }

        void addRewriteMap(string originName, string newName)
        {
            this->rewriteMap[originName] = newName;
        }

        string getNewName(string originName)
        {
            return this->rewriteMap[originName];
        }

    };


    shared_ptr<ExceptionCollector> exceptionCollector;
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
    shared_ptr<RewriteContext> rewriteContext = make_shared<RewriteContext>(make_shared<RowExpressionTreeRewriter>(
            make_shared<RowExpressionNodeAllocator>()));

public:
    OptimizeOutputProject()
    {
        exceptionCollector = make_shared<ExceptionCollector>();
        functionAndTypeResolver = make_shared<FunctionAndTypeResolver>(exceptionCollector);
    }

    PlanNode* optimize(PlanNode *plan,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                       shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator)
    {
        shared_ptr<Visitor> visitor = make_shared<Visitor>(variableAllocator,planNodeAllocator,this->exceptionCollector,functionAndTypeResolver);
        return (PlanNode *)visitor->Visit(plan,rewriteContext.get());
    }


    class Visitor : public SimplePlanNodeRewriter
    {
        shared_ptr<VariableAllocator> variableAllocator;
        shared_ptr<PlanNodeAllocator> planNodeAllocator;
        shared_ptr<ExceptionCollector> exceptionCollector;
        shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;

    public:
        Visitor(shared_ptr<VariableAllocator> variableAllocator,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                shared_ptr<ExceptionCollector> exceptionCollector,shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver)  : SimplePlanNodeRewriter(planNodeAllocator) {
            this->variableAllocator = variableAllocator;
            this->planNodeAllocator = planNodeAllocator;
            this->exceptionCollector = exceptionCollector;
            this->functionAndTypeResolver = functionAndTypeResolver;

        }


        void * VisitProjectNode(ProjectNode *node, void *context) override {

            auto rewritten = (PlanNode *) Visit(node->getSource(), context);

            auto reContext = (RewriteContext*)context;

            if (rewritten->getType() == "FinalAggregationNode") {

                shared_ptr<AssignmentsBuilder> assignmentsBuilder = make_shared<AssignmentsBuilder>();
                auto assignments = node->getAssignments();

                for(auto assignment : assignments->getMap())
                {
                    auto newAssign = (RowExpression*)reContext->rewrite(assignment.second.get(),reContext);
                    assignmentsBuilder->put(assignment.first,shared_ptr<RowExpression>(newAssign));
                }

                auto newProject = this->planNodeAllocator->new_ProjectNode(node->getId(),rewritten,assignmentsBuilder->build());
                return newProject;
            }

            return this->planNodeAllocator->record(node->replaceChildren({rewritten}));
        }

    };

};


#endif //OLVP_OPTIMIZEOUTPUTPROJECT_HPP
