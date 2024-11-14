//
// Created by zxk on 11/8/24.
//

#ifndef FRONTEND_ADDEXCHANGE_HPP
#define FRONTEND_ADDEXCHANGE_HPP



#include "PlanOptimizer.h"
#include "SimplePlanNodeRewriter.hpp"
#include "../Analyzer/FunctionAndTypeResolver.hpp"
#include "../Planner/Expression/RowExpressionTreeRewriter.hpp"

class AddExchange : public PlanOptimizer
{

    class RewriteContext
    {
        bool finalAgg = false;
    public:
        enum JoinSide{PROBE,BUILD,NO_JOIN};
        JoinSide joinSide = NO_JOIN;

        void setJoinSide(RewriteContext::JoinSide joinSide)
        {
            this->joinSide = joinSide;
        }

        void setFinalAgg()
        {
            this->finalAgg = true;
        }

        bool hasFinalAgg()
        {
            return this->finalAgg;
        }
    };

public:
    AddExchange()
    {

    }

    PlanNode* optimize(PlanNode *plan,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                       shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator)
    {
        shared_ptr<Visitor> visitor = make_shared<Visitor>(variableAllocator,planNodeAllocator,idAllocator);

        shared_ptr<RewriteContext> rewriteContext = make_shared<RewriteContext>();
        return (PlanNode *)visitor->Visit(plan,rewriteContext.get());
    }


    class Visitor : public SimplePlanNodeRewriter
    {
        shared_ptr<VariableAllocator> variableAllocator;
        shared_ptr<PlanNodeAllocator> planNodeAllocator;
        shared_ptr<PlanNodeIdAllocator> idAllocator;


    public:
        Visitor(shared_ptr<VariableAllocator> variableAllocator,shared_ptr<PlanNodeAllocator> planNodeAllocator,shared_ptr<PlanNodeIdAllocator> idAllocator)  : SimplePlanNodeRewriter(planNodeAllocator) {

            this->variableAllocator = variableAllocator;
            this->planNodeAllocator = planNodeAllocator;
            this->idAllocator = idAllocator;

        }


        void * VisitFinalAggregationNode(FinalAggregationNode *node, void *context) override
        {
            auto reContext = (RewriteContext*)context;
            reContext->setFinalAgg();
            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);
            auto exhcange = this->planNodeAllocator->new_GatherExchangeNode(idAllocator->getNextId(),rewrittenNode);

            return this->planNodeAllocator->record(node->replaceChildren({exhcange}));
        }

        void * VisitTaskOutputNode(TaskOutputNode *node, void *context) override{


            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            auto reContext = (RewriteContext*)context;
            if(!reContext->hasFinalAgg()) {

                auto exhcange = this->planNodeAllocator->new_GatherExchangeNode(idAllocator->getNextId(),
                                                                                rewrittenNode);
                return node->replaceChildren({exhcange});
            }
            return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));

        }

        void * VisitLookupJoinNode(LookupJoinNode *node, void *context) override
        {

            auto reContext = (RewriteContext*)context;

            reContext->joinSide = RewriteContext::BUILD;
            PlanNode *rewrittenNodeBuild = (PlanNode *) Visit(node->getBuild(), context);

            reContext->joinSide = RewriteContext::PROBE;
            PlanNode *rewrittenNodeProbe = (PlanNode *) Visit(node->getProbe(), context);


            auto buildExchange = this->planNodeAllocator->new_ReplicatedExchangeNode(idAllocator->getNextId(),rewrittenNodeBuild);
            return this->planNodeAllocator->new_LookupJoinNode(idAllocator->getNextId(),rewrittenNodeProbe,buildExchange,node->getJoinClauses(),node->getOutputVariables());

        }
        void * VisitTableScanNode(TableScanNode *node, void *context) override
        {
            auto reContext = (RewriteContext*)context;

            if(reContext->joinSide == RewriteContext::PROBE || reContext->joinSide == RewriteContext::NO_JOIN) {
                auto exchangeNode = this->planNodeAllocator->new_RoundRobinExchangeNode(idAllocator->getNextId(), node);
                return exchangeNode;
            }

            return node;
        }

    };

};





#endif //FRONTEND_ADDEXCHANGE_HPP
