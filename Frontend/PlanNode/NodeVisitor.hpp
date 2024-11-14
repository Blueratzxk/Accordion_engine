//
// Created by zxk on 10/22/24.
//

#ifndef FRONTEND_NODEVISITOR_HPP
#define FRONTEND_NODEVISITOR_HPP
#include "PlanNode.hpp"
class ExchangeNode;
class FilterNode;
class FinalAggregationNode;
class LookupJoinNode;
class TaskOutputNode;
class PartialAggregationNode;
class SortNode;
class ProjectNode;
class RemoteSourceNode;
class TableScanNode;
class TopKNode;
class LocalExchangeNode;
class CrossJoinNode;
class LimitNode;
class AggregationNode;

class NodeVisitor {

public:
    //virtual ~NodeVisitor();


    virtual void* Visit(PlanNode* node,void *context) {
        return node->accept(this,context);
    }
    virtual void* VisitExchangeNode(ExchangeNode* node,void *context) = 0;
    virtual void* VisitFilterNode(FilterNode* node,void *context) = 0;
    virtual void* VisitFinalAggregationNode(FinalAggregationNode* node,void *context) = 0;
    virtual void* VisitAggregationNode(AggregationNode* node,void *context) = 0;
    virtual void* VisitLookupJoinNode(LookupJoinNode* node,void *context) = 0;
    virtual void* VisitCrossJoinNode(CrossJoinNode* node,void *context) = 0;
    virtual void* VisitTaskOutputNode(TaskOutputNode* node,void *context) = 0;
    virtual void* VisitPartialAggregationNode(PartialAggregationNode* node,void *context) = 0;
    virtual void* VisitSortNode(SortNode* node,void *context) = 0;
    virtual void* VisitProjectNode(ProjectNode* node,void *context) = 0;
    virtual void* VisitRemoteSourceNode(RemoteSourceNode* node,void *context) = 0;
    virtual void* VisitTableScanNode(TableScanNode* node,void *context) = 0;
    virtual void* VisitTopKNode(TopKNode* node,void *context) = 0;
    virtual void* VisitLocalExchangeNode(LocalExchangeNode* node,void *context) = 0;
    virtual void* VisitLimitNode(LimitNode* node,void *context) = 0;

};
#endif //FRONTEND_NODEVISITOR_HPP
