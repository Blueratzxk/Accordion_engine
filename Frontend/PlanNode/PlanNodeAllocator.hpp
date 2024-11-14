//
// Created by zxk on 11/4/24.
//

#ifndef FRONTEND_PLANNODEALLOCATOR_HPP
#define FRONTEND_PLANNODEALLOCATOR_HPP

#include "PlanNodeTree.hpp"

class ColumnHandle;
class TableHandle;

#include <set>
class PlanNodeAllocator
{
    set<PlanNode*> planNodes;

    int addNums = 0;
    int releaseNums = 0;

public:
    PlanNodeAllocator(){

    }

    void addNode(PlanNode *node)
    {
        planNodes.insert(node);
        addNums++;
    }

    ExchangeNode* new_GatherExchangeNode(string id,PlanNode *child) {
        auto node = ExchangeNode::getGatheringExchangeNode(id,child);

        addNode(node);
        return node;
    }

    ExchangeNode* new_ReplicatedExchangeNode(string id,PlanNode *child) {
        auto node = ExchangeNode::getReplicatedExchangeNode(id,child);

        addNode(node);
        return node;
    }

    ExchangeNode* new_RoundRobinExchangeNode(string id,PlanNode *child) {
        auto node = ExchangeNode::getRoundRobinExchangeNode(id,child);

        addNode(node);
        return node;
    }

    FilterNode* new_FilterNode(string location,string id, PlanNode *source,shared_ptr<RowExpression> predicates)
    {
        auto node = new FilterNode(location,id,source,predicates);

        addNode(node);
        return node;
    }

    FinalAggregationNode* new_FinalAggregationNode(string id,    PlanNode *source, map<shared_ptr<VariableReferenceExpression>,
            shared_ptr<Aggregation>> aggregations, list<shared_ptr<VariableReferenceExpression>> groupByKeys)
    {
        auto node = new FinalAggregationNode(id,source,aggregations,groupByKeys);

        addNode(node);
        return node;
    }

    AggregationNode* new_AggregationNode(string id, PlanNode *source, map<shared_ptr<VariableReferenceExpression>,
            shared_ptr<Aggregation>> aggregations, list<shared_ptr<VariableReferenceExpression>> groupByKeys)
    {
        auto node = new AggregationNode(id,source,aggregations,groupByKeys);

        addNode(node);
        return node;
    }

    LookupJoinNode* new_LookupJoinNode(string id, PlanNode *probe, PlanNode *build,list<shared_ptr<EquiJoinClause>> joinClauses,vector<shared_ptr<VariableReferenceExpression>> outputVariables)
    {
        auto node = new LookupJoinNode(id,probe,build,joinClauses,outputVariables);

        addNode(node);
        return node;
    }

    CrossJoinNode* new_CrossJoinNode(string id) {

        auto node = new CrossJoinNode(id);

        addNode(node);
        return node;

    }
    TaskOutputNode* new_TaskOutputNode(string id)
    {
        auto node = new TaskOutputNode(id);

        addNode(node);
        return node;
    }

    PartialAggregationNode* new_PartialAggregationNode(string id,    PlanNode *source, map<shared_ptr<VariableReferenceExpression>,
            shared_ptr<Aggregation>> aggregations, list<shared_ptr<VariableReferenceExpression>> groupByKeys)
    {
        auto node = new PartialAggregationNode(id,source,aggregations,groupByKeys);

        addNode(node);
        return node;
    }

    SortNode* new_SortNode(string id)
    {
        auto node = new SortNode(id);

        addNode(node);
        return node;
    }

    ProjectNode* new_ProjectNode(string id,PlanNode *source,shared_ptr<Assignments> assignments)
    {
        auto node = new ProjectNode(id,source,assignments);

        addNode(node);
        return node;
    }

    RemoteSourceNode* new_RemoteSourceNode(string id)
    {
        auto node = new RemoteSourceNode(id);

        addNode(node);
        return node;
    }

    TableScanNode* new_TableScanNode(string location, string id,shared_ptr<TableHandle> tableHandle,
                                     vector<shared_ptr<VariableReferenceExpression>> outputVariables,
                                     map<shared_ptr<VariableReferenceExpression>,shared_ptr<ColumnHandle>> columns)
    {
        auto node = new TableScanNode(location,id,tableHandle,outputVariables,columns);

        addNode(node);
        return node;
    }

    TopKNode* new_TopKNode(string id)
    {
        auto node = new TopKNode(id);

        addNode(node);
        return node;
    }

    LocalExchangeNode* new_LocalExchangeNode(string id)
    {
        auto node = new LocalExchangeNode(id);

        addNode(node);
        return node;
    }

    LimitNode* new_LimitNode(string id,int limit)
    {
        auto node = new LimitNode(id,limit);

        addNode(node);
        return node;
    }

    void carvePlanTree(PlanNode * root)
    {

        auto childs = root->getSources();
        for(auto child : childs)
            carvePlanTree(child);

        this->planNodes.erase(root);
    }

    PlanNode* record(PlanNode *node)
    {
        if(!this->planNodes.contains(node)) {
            addNums++;
            this->planNodes.insert(node);
        }
        return node;
    }

    void release_all()
    {
        for(auto node : this->planNodes){
            releaseNums++;
            delete node;
        }
    }


};
#endif //FRONTEND_PLANNODEALLOCATOR_HPP
