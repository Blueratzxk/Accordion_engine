//
// Created by zxk on 11/1/24.
//

#ifndef FRONTEND_AGGREGATIONNODE_HPP
#define FRONTEND_AGGREGATIONNODE_HPP

#include "../Planner/Aggregation.hpp"
class AggregationNode : public PlanNode
{
    string id;
    PlanNode *source;
    map<shared_ptr<VariableReferenceExpression>, shared_ptr<Aggregation>> aggregations;
    list<shared_ptr<VariableReferenceExpression>> groupByKeys;
    vector<shared_ptr<VariableReferenceExpression>> outputs;
public:

    AggregationNode(string id,AggregationDesc desc):PlanNode("FinalAggregationNode",id) {
       
    }

    AggregationNode(string id,    PlanNode *source, map<shared_ptr<VariableReferenceExpression>,
            shared_ptr<Aggregation>> aggregations, list<shared_ptr<VariableReferenceExpression>> groupByKeys):PlanNode("AggregationNode",id) {

        this->id = id;
        this->source = source;
        this->aggregations = aggregations;
        this->groupByKeys = groupByKeys;


        for(auto groupBy : groupByKeys)
        {
            outputs.push_back(groupBy);
        }

        for(auto agg : aggregations)
        {
            outputs.push_back(agg.first);
        }

    }

    vector<AggregateDesc> getAggregationDesc()
    {
        return {};
    }

    void* accept(NodeVisitor* visitor,void *context)  {
        return visitor->VisitAggregationNode(this,context);
    }

    list<shared_ptr<VariableReferenceExpression>> getGroupByKeys()
    {
        return this->groupByKeys;
    }
    map<shared_ptr<VariableReferenceExpression>, shared_ptr<Aggregation>> getAggregations()
    {
        return this->aggregations;
    }

    string getId()
    {
        return PlanNode::getId();
    }

    vector<shared_ptr<VariableReferenceExpression>> getOutputVariables() override{
        return outputs;
    }

    void addSource(PlanNode *node)
    {
        this->source = node;
    }
    void addSources(PlanNode *node){}
    PlanNode* getSource(){
        return this->source;
    }
    vector<PlanNode*> getSources(){
        vector<PlanNode*> sources{this->source};
        return sources;
    }
    PlanNode* replaceChildren(vector<PlanNode*> newChildren){
        AggregationNode *agg = new AggregationNode(id,source,aggregations,groupByKeys);
        agg->addSource(newChildren[0]);
        return agg;
    }

};

#endif //FRONTEND_AGGREGATIONNODE_HPP
