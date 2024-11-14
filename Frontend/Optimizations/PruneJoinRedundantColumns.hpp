//
// Created by zxk on 11/6/24.
//

#ifndef FRONTEND_PRUNEJOINREDUNDANTCOLUMNS_HPP
#define FRONTEND_PRUNEJOINREDUNDANTCOLUMNS_HPP

#include "PlanOptimizer.h"
#include "SimplePlanNodeRewriter.hpp"
#include "../Planner/Expression/RowExpressionTreeRewriter.hpp"

class PruneJoinRedundantColumns : public PlanOptimizer
{

    class PruneContext {
        shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter;
        set<string> columnNames;

        bool star = false;

    public:
        PruneContext(shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter) {
            this->rowExpressionTreeRewriter = rowExpressionTreeRewriter;
        }

        void recordStar()
        {
            this->star = true;
        }
        void eraseStar()
        {
            this->star = false;
        }

        bool hasStar() const{
            return this->star;
        }

        void addColumnName(string name)
        {
            if(!this->columnNames.contains(name))
                this->columnNames.insert(name);
        }

        bool containColumn(string name)
        {
            return this->columnNames.contains(name);
        }

    };





    shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter;
public:
    PruneJoinRedundantColumns()
    {

    }

    PlanNode* optimize(PlanNode *plan,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                       shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator)
    {

        this->rowExpressionTreeRewriter = make_shared<RowExpressionTreeRewriter>(make_shared<RowExpressionNodeAllocator>());
        PruneContext *context = new PruneContext(this->rowExpressionTreeRewriter);



        shared_ptr<Visitor> visitor = make_shared<Visitor>(variableAllocator,planNodeAllocator);
        auto result = (PlanNode *)visitor->Visit(plan,context);


        delete context;
        return result;
    }


    class RowExprVisitor:public DefaultRowExpressionVisitor {

    public:
        RowExprVisitor() {

        }

        void *VisitCall(CallExpression *call, void *context) override {
            auto pruneContext = (PruneContext *) context;

            if(call->getArguments().empty())
                pruneContext->recordStar();

            for (auto argument: call->getArguments())
                Visit(argument.get(), context);

            pruneContext->addColumnName(call->getDisplayName());
            return NULL;
        }

        void *VisitVariableReference(VariableReferenceExpression *reference, void *context) override {
            auto pruneContext = (PruneContext *) context;
            pruneContext->addColumnName(reference->getName());

            return NULL;
        }

    };


    class Visitor : public SimplePlanNodeRewriter {

        shared_ptr<VariableAllocator> variableAllocator;
        shared_ptr<PlanNodeAllocator> planNodeAllocator;
    public:
        Visitor(shared_ptr<VariableAllocator> variableAllocator,shared_ptr<PlanNodeAllocator> planNodeAllocator) : SimplePlanNodeRewriter(planNodeAllocator) {
            this->variableAllocator = variableAllocator;
            this->planNodeAllocator = planNodeAllocator;
        }


        void * VisitFinalAggregationNode(FinalAggregationNode *node, void *context) override
        {
            shared_ptr<RowExprVisitor> rowExprVisitor = make_shared<RowExprVisitor>();


            for(auto agg : node->getAggregations())
            {
                rowExprVisitor->Visit(agg.first.get(),context);
                rowExprVisitor->Visit(agg.second->getCall().get(),context);
            }

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            auto pruneContext = (PruneContext *) context;
            pruneContext->eraseStar();

            return node->replaceChildren({rewrittenNode});
        }

        void * VisitPartialAggregationNode(PartialAggregationNode *node, void *context) override
        {
            shared_ptr<RowExprVisitor> rowExprVisitor = make_shared<RowExprVisitor>();


            for(auto agg : node->getAggregations())
            {
                rowExprVisitor->Visit(agg.first.get(),context);
                rowExprVisitor->Visit(agg.second->getCall().get(),context);
            }

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            auto pruneContext = (PruneContext *) context;
            pruneContext->eraseStar();

            return node->replaceChildren({rewrittenNode});
        }

        void * VisitAggregationNode(AggregationNode *node, void *context) override
        {
            shared_ptr<RowExprVisitor> rowExprVisitor = make_shared<RowExprVisitor>();


            for(auto agg : node->getAggregations())
            {
                rowExprVisitor->Visit(agg.first.get(),context);
                rowExprVisitor->Visit(agg.second->getCall().get(),context);
            }

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            auto pruneContext = (PruneContext *) context;
            pruneContext->eraseStar();

            return node->replaceChildren({rewrittenNode});
        }


        void *VisitCrossJoinNode(CrossJoinNode *node, void *context) override {

            PruneContext* pruneContext = (PruneContext*)context;
            PlanNode *rewrittenNodeBuild = (PlanNode *) Visit(node->getBuild(), context);
            PlanNode *rewrittenNodeProbe = (PlanNode *) Visit(node->getProbe(), context);

            vector<shared_ptr<VariableReferenceExpression>> outputVariables;
            auto outputs = node->getOutputVariables();

            for(auto output : outputs)
            {
                if(pruneContext->containColumn(output->getNameOrValue()))
                    outputVariables.push_back(output);
            }

            auto newNode = this->planNodeAllocator->new_CrossJoinNode(node->getId());

            return this->planNodeAllocator->record(newNode->replaceChildren({rewrittenNodeProbe, rewrittenNodeBuild}));

        }

        void * VisitLookupJoinNode(LookupJoinNode *node, void *context) override
        {
            PruneContext* pruneContext = (PruneContext*)context;


            shared_ptr<RowExprVisitor> rowExprVisitor = make_shared<RowExprVisitor>();
            for(auto expr : node->getJoinClauses())
            {
                rowExprVisitor->Visit(expr->getLeft().get(),context);
                rowExprVisitor->Visit(expr->getRight().get(),context);
            }



            vector<shared_ptr<VariableReferenceExpression>> outputVariables;
            auto outputs = node->getOutputVariables();

            if(pruneContext->hasStar()){
                outputVariables = node->getOutputVariables();
            }
            else {
                for (auto output: outputs) {
                    if (pruneContext->containColumn(output->getNameOrValue()))
                        outputVariables.push_back(output);
                }
            }

            PlanNode *rewrittenNodeBuild = (PlanNode *) Visit(node->getBuild(), context);
            PlanNode *rewrittenNodeProbe = (PlanNode *) Visit(node->getProbe(), context);

            auto newNode = this->planNodeAllocator->new_LookupJoinNode(node->getId(),node->getProbe(),node->getBuild(),node->getJoinClauses(),outputVariables);

            return this->planNodeAllocator->record(newNode->replaceChildren({rewrittenNodeProbe, rewrittenNodeBuild}));
        }


        void * VisitFilterNode(FilterNode *node, void *context) override
        {

            PruneContext* pruneContext = (PruneContext*)context;

            shared_ptr<RowExprVisitor> rowExprVisitor = make_shared<RowExprVisitor>();
            rowExprVisitor->Visit(node->getPredicates().get(),context);

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));

        }


        void *VisitProjectNode(ProjectNode *node, void *context) override {


            PruneContext* pruneContext = (PruneContext*)context;
            auto assignments = node->getAssignments()->getMap();

            if(node->getSource()->getType() != "LookupJoinNode" && node->getSource()->getType() != "TableScanNode")
            {
                for(auto assignment : assignments)
                {

                    pruneContext->addColumnName(assignment.second->getNameOrValue());
                }
            }


            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            node->addSource(rewrittenNode);
            return node;
        }





    };


};







#endif //FRONTEND_PRUNEJOINREDUNDANTCOLUMNS_HPP
