//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_FRAGMENTER_HPP
#define OLVP_FRAGMENTER_HPP

#include "Fragment.hpp"
#include "../Frontend/PlanNode/PlanNodeTree.hpp"

class PlanNodeTreeBreaker : public NodeVisitor {

private:
    int nextNodeUniqueId = -1;

public:
    PlanNodeTreeBreaker() {

    }
    string getNextNodeId()
    {
        this->nextNodeUniqueId++;
        return to_string(this->nextNodeUniqueId);
    }


    void* VisitExchangeNode(ExchangeNode* node,void *context) override{

        shared_ptr<PartitioningScheme> scheme = node->getPartitioningScheme();

        if(node->getExchangeType() == ExchangeNode::GATHER)
        {
            ((FragmentProperties*)context)->setSingleNodeDistribution();
        }
        else if (node->getExchangeType() == ExchangeNode::REPARTITION) {
            ((FragmentProperties*)context)->setDistribution(scheme->getPartitioning()->getHandle());
        }
        else if (node->getExchangeType() == ExchangeNode::REPLICATE) {
            ((FragmentProperties*)context)->setDistribution(scheme->getPartitioning()->getHandle());
        }


        string fragmentId = ((FragmentProperties*)context)->getNextFragmentId();

        FragmentProperties *newProperties = new FragmentProperties(scheme,((FragmentProperties*)context)->getIdAddr());
        PlanNode *child =(PlanNode*)Visit(node->getSource(),newProperties);



        std::shared_ptr<SubPlan> sp = createSubPlan(child,fragmentId,newProperties);


        int fid = *((FragmentProperties*)context)->getIdAddr();
        this->markLabels(node->getSources(),fragmentId);

        delete newProperties;

        ((FragmentProperties*)context)->addChildren({sp});

        RemoteSourceNode *remote = new RemoteSourceNode("RemoteSource_"+getNextNodeId());
        remote->setSourceFragmentId(sp->getFragment().getFragmentId());

        return remote;
    }
    void*  VisitFilterNode(FilterNode* node,void *context)override{

        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);


        return node->replaceChildren({child});
    }

    void*  VisitFinalAggregationNode(FinalAggregationNode* node,void *context) override{

        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);


        return node->replaceChildren({child});
    }
    void*  VisitLookupJoinNode(LookupJoinNode* node,void *context) override{

        PlanNode *childLeft = (PlanNode*)Visit(node->getProbe(),context);
        PlanNode *childRight = (PlanNode*)Visit(node->getBuild(),context);



        return node->replaceChildren({childLeft,childRight});

    }
    void*  VisitCrossJoinNode(CrossJoinNode* node,void *context) override{

        PlanNode *childLeft = (PlanNode*)Visit(node->getProbe(),context);
        PlanNode *childRight = (PlanNode*)Visit(node->getBuild(),context);


        return node->replaceChildren({childLeft,childRight});

    }
    void* VisitSortNode(SortNode* node,void *context) override{

        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);


        return node->replaceChildren({child});
    }
    void*  VisitTaskOutputNode(TaskOutputNode* node,void *context) override{

        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);


        return node->replaceChildren({child});
    }
    void*  VisitPartialAggregationNode(PartialAggregationNode* node,void *context) override{

        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);



        return node->replaceChildren({child});
    }
    void*  VisitProjectNode(ProjectNode* node,void *context)override {

        PlanNode *child =(PlanNode*) Visit(node->getSource(),context);


        return node->replaceChildren({child});
    }
    void*  VisitLimitNode(LimitNode* node,void *context)override {

        PlanNode *child =(PlanNode*) Visit(node->getSource(),context);


        return node->replaceChildren({child});
    }
    void*  VisitRemoteSourceNode(RemoteSourceNode* node,void *context) override{


        return node->replaceChildren({});
    }
    void* VisitTableScanNode(TableScanNode* node,void *context) override{


        return node->replaceChildren({});
    }
    void*  VisitTopKNode(TopKNode* node,void *context) override{

        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);


        return node->replaceChildren({child});
    }
    void* VisitLocalExchangeNode(LocalExchangeNode* node,void *context) override{

        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);


        return node->replaceChildren({child});

    }

    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        PlanNode *child = (PlanNode*)Visit(node->getSource(),context);


        return node->replaceChildren({child});

    }

    std::shared_ptr<SubPlan> createSubPlan(PlanNode *node,string id,FragmentProperties *properties)
    {
        PlanFragment fragment(node,id,((FragmentProperties*)properties)->getPartitioningScheme(),((FragmentProperties*)properties)->getHandle().value());
        std::shared_ptr<SubPlan> sp = std::make_shared<SubPlan>(fragment,((FragmentProperties*)properties)->getChildren());
        return sp;
    }

    static void markLabels(vector<PlanNode *>nodes,string label)
    {

        for(auto node : nodes)
        {
            if(node->getType() == "ExchangeNode")
                continue;
            node->setLabel(label);
            if(node->getType() == "TableScanNode")
                break;
            markLabels(node->getSources(),label);

        }

    }

};



class Fragmenter
{
    PlanNode *planTreeRoot;

    FragmentProperties *fragmentProperties;
    PlanNodeTreeBreaker evaluator;
    std::shared_ptr<SubPlan> subPlan;
    int idGenerator = -1;

public:
    Fragmenter(PlanNode *root){

        fragmentProperties = new FragmentProperties(&this->idGenerator);
        this->planTreeRoot = root;
    }
    Fragmenter(){}
    void setRoot(PlanNode *root)
    {
        fragmentProperties = new FragmentProperties(&this->idGenerator);
        this->planTreeRoot = root;
    }

    void doFragment()
    {


        string fragmentId = ((FragmentProperties*)fragmentProperties)->getNextFragmentId();

        PlanNode *fragRoot = (PlanNode*)evaluator.Visit(planTreeRoot,fragmentProperties);


        std::shared_ptr<SubPlan> sp = evaluator.createSubPlan(fragRoot,fragmentId,fragmentProperties);

        PlanNodeTreeBreaker::markLabels({this->planTreeRoot}, fragmentId);


        this->subPlan = sp;

    }

    void renumberFragmentId(int id,std::shared_ptr<SubPlan> sp)
    {
        sp->setFragmentId(to_string(id));
        if(sp->getChildren().empty())
            return;
        for(auto child : sp->getChildren())
        {
            renumberFragmentId(++id,child);
        }
    }
    vector<PlanFragment>  getFragments()
    {
        return this->subPlan->getAllFragments();
    }
    std::shared_ptr<SubPlan> getSubPlan()
    {
        return this->subPlan;
    }


};





#endif //OLVP_FRAGMENTER_HPP
