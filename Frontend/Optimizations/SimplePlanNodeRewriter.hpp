//
// Created by zxk on 11/5/24.
//

#ifndef FRONTEND_SIMPLEPLANNODEREWRITER_HPP
#define FRONTEND_SIMPLEPLANNODEREWRITER_HPP

#include "../PlanNode/NodeVisitor.hpp"
#include "../PlanNode/PlanNodeTree.hpp"

class SimplePlanNodeRewriter : public NodeVisitor
{
    shared_ptr<PlanNodeAllocator> planNodeAllocator;

    list<PlanNode *> stack;

public:
    SimplePlanNodeRewriter(shared_ptr<PlanNodeAllocator> planNodeAllocator){
        this->planNodeAllocator = planNodeAllocator;
    }

    list<PlanNode *> getStack(){
        return this->stack;
    }

    PlanNode *getParent()
    {
        if(stack.size() <= 1)
            return NULL;

        auto it = stack.rbegin();
        it++;
        return *it;
    }

    void *Visit(PlanNode* node,void *context) override{

        stack.push_back(node);
        auto result = node->accept(this,context);
        stack.pop_back();
        return result;
    }
    void* VisitExchangeNode(ExchangeNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }
    void* VisitFilterNode(FilterNode* node,void *context)override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);
        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }

    void* VisitFinalAggregationNode(FinalAggregationNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(),context);
        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }
    void* VisitLookupJoinNode(LookupJoinNode* node,void *context) override{

        PlanNode *rewrittenNodeBuild = (PlanNode *)Visit(node->getBuild(),context);
        PlanNode *rewrittenNodeProbe = (PlanNode *)Visit(node->getProbe(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNodeProbe,rewrittenNodeBuild}));

    }
    void* VisitCrossJoinNode(CrossJoinNode* node,void *context) override{

        PlanNode *rewrittenNodeBuild = (PlanNode *)Visit(node->getBuild(),context);
        PlanNode *rewrittenNodeProbe = (PlanNode *)Visit(node->getProbe(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNodeProbe,rewrittenNodeBuild}));

    }

    void* VisitSortNode(SortNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }
    void* VisitTaskOutputNode(TaskOutputNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }
    void* VisitPartialAggregationNode(PartialAggregationNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }


    void* VisitProjectNode(ProjectNode* node,void *context)override {

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }

    void* VisitLimitNode(LimitNode* node,void *context)override {

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }

    void* VisitRemoteSourceNode(RemoteSourceNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));

    }
    void* VisitTableScanNode(TableScanNode* node,void *context) override{

        return node;
    }
    void* VisitTopKNode(TopKNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }
    void* VisitLocalExchangeNode(LocalExchangeNode* node,void *context) override{

        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));

    }
    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        PlanNode *rewrittenNode = (PlanNode *)Visit(node->getSource(),context);

        return this->planNodeAllocator->record(node->replaceChildren({rewrittenNode}));
    }



};



#endif //FRONTEND_SIMPLEPLANNODEREWRITER_HPP
