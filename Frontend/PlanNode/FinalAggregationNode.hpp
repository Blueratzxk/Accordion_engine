//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_FINALAGGREGATIONNODE_HPP
#define OLVP_FINALAGGREGATIONNODE_HPP

#include "../Planner/Aggregation.hpp"
#include "../../Descriptor/AggregationDescriptor.hpp"
#include "../RowExpressionToAstExpression.hpp"
class FinalAggregationNode : public PlanNode
{
    string id;
    PlanNode *source;
    map<shared_ptr<VariableReferenceExpression>, shared_ptr<Aggregation>> aggregations;
    list<shared_ptr<VariableReferenceExpression>> groupByKeys;
    vector<shared_ptr<VariableReferenceExpression>> outputs;
    AggregationDesc desc;

    bool planPhase = false;
public:

    FinalAggregationNode(string id,AggregationDesc desc):PlanNode("FinalAggregationNode",id) {
        this->desc = desc;
    }


    FinalAggregationNode(string id,    PlanNode *source, map<shared_ptr<VariableReferenceExpression>,
            shared_ptr<Aggregation>> aggregations, list<shared_ptr<VariableReferenceExpression>> groupByKeys):PlanNode("FinalAggregationNode",id) {

        planPhase = true;

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
    AggregationDesc getAggregationDesc()
    {
        return this->desc;
    }

    void* accept(NodeVisitor* visitor,void *context)  {
        return visitor->VisitFinalAggregationNode(this,context);
    }

    map<shared_ptr<VariableReferenceExpression>, shared_ptr<Aggregation>> getAggregations()
    {
        return this->aggregations;
    }
    list<shared_ptr<VariableReferenceExpression>> getGroupByKeys()
    {
        return this->groupByKeys;
    }

    vector<shared_ptr<VariableReferenceExpression>> getOutputVariables() override
    {
        return this->outputs;
    }

    string getId()
    {
        return PlanNode::getId();
    }

    void transform() override
    {
        this->planPhase = false;
       // AggregateDesc(string functionName,string inputKey,string outputName)
        vector<AggregateDesc> aggregates;
        vector<string> groupByKeys;

        bool hasGroupBy = (!this->groupByKeys.empty());

        RowExpressionToAstExpression rowExpressionToAstExpression;

        for(auto agg : this->aggregations)
        {
            string functionName = dynamic_pointer_cast<AggregationFunctionHandle>(agg.second->getCall()->getFunctionHandle())->getFunctionId();
            if(hasGroupBy)
                functionName = functionName+"_groupBy";

            functionName = rowExpressionToAstExpression.getArrowFunctionName(functionName);

            string inputKey = "";
            if(!agg.second->getCall()->getArguments().empty()) {
                inputKey = agg.second->getCall()->getArguments().front()->getNameOrValue();
            }

          //  rowExpressionToAstExpression.Visit(agg.second->getCall().get(),NULL);
          //  string outputName = rowExpressionToAstExpression.getExpressionString();
          //  rowExpressionToAstExpression.resetExpressionString();

            string outputName = agg.first->getNameOrValue();
            AggregateDesc aggDesc(functionName,inputKey,outputName);
            aggregates.push_back(aggDesc);
        }

        for(auto groupByKey : this->groupByKeys)
            groupByKeys.push_back(groupByKey->getName());

        this->desc = AggregationDesc(aggregates,groupByKeys);


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

        if(!planPhase)
        {
            FinalAggregationNode *fagg = new FinalAggregationNode(this->getId(),this->getAggregationDesc());
            fagg->addSource(newChildren[0]);
            return fagg;
        }

        FinalAggregationNode *fagg = new FinalAggregationNode(id,source,aggregations,groupByKeys);
        fagg->addSource(newChildren[0]);
        return fagg;
    }

};

#endif //OLVP_FINALAGGREGATIONNODE_HPP
