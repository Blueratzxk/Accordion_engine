//
// Created by zxk on 11/1/24.
//

#ifndef FRONTEND_PLANNODETREECLEANER_HPP
#define FRONTEND_PLANNODETREECLEANER_HPP


class PlanNodeTreeCleaner : public NodeVisitor {

private:

public:
    PlanNodeTreeCleaner() {
    }

    void* VisitExchangeNode(ExchangeNode* node,void *context) override{


        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;

    }
    void* VisitFilterNode(FilterNode* node,void *context)override{

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }

    void* VisitFinalAggregationNode(FinalAggregationNode* node,void *context) override{

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }
    void* VisitLookupJoinNode(LookupJoinNode* node,void *context) override{

        Visit(node->getBuild(),context);
        Visit(node->getProbe(),context);

        delete node;
        node = NULL;
        return NULL;

    }
    void* VisitCrossJoinNode(CrossJoinNode* node,void *context) override{

        Visit(node->getBuild(),context);
        Visit(node->getProbe(),context);

        delete node;
        node = NULL;
        return NULL;

    }

    void* VisitSortNode(SortNode* node,void *context) override{

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }
    void* VisitTaskOutputNode(TaskOutputNode* node,void *context) override{

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }
    void* VisitPartialAggregationNode(PartialAggregationNode* node,void *context) override{

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }


    void* VisitProjectNode(ProjectNode* node,void *context)override {

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }

    void* VisitLimitNode(LimitNode* node,void *context)override {

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }

    void* VisitRemoteSourceNode(RemoteSourceNode* node,void *context) override{
        delete node;
        node = NULL;
        return NULL;

    }
    void* VisitTableScanNode(TableScanNode* node,void *context) override{
        delete node;
        node = NULL;
        return NULL;
    }
    void* VisitTopKNode(TopKNode* node,void *context) override{

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }
    void* VisitLocalExchangeNode(LocalExchangeNode* node,void *context) override{

        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;

    }
    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        Visit(node->getSource(),context);
        delete node;
        node = NULL;
        return NULL;
    }

};

#endif //FRONTEND_PLANNODETREECLEANER_HPP
