//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_TOPKNODE_HPP
#define OLVP_TOPKNODE_HPP

#include "../../Descriptor/TopKDescriptor.hpp"
class TopKNode : public PlanNode
{
    PlanNode *source;
    TopKDescriptor desc;

    bool planPhase = false;
public:

    TopKNode (string id) :PlanNode("TopKNode",id)
    {
        planPhase = true;
    }
    TopKNode (string id,TopKDescriptor desc) :PlanNode("TopKNode",id)
    {
        this->desc = desc;
    }


    TopKDescriptor getTopKDescriptor()
    {
        return this->desc;
    }
    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitTopKNode(this,context);
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
    string getId()
    {
        return PlanNode::getId();
    }


    PlanNode* replaceChildren(vector<PlanNode*> newChildren){

        if(!planPhase)
        {
            TopKNode *topk = new TopKNode(this->getId(),desc);
            topk->addSource(newChildren[0]);
            return topk;
        }

        TopKNode *topk = new TopKNode(this->getId());
        topk->addSource(newChildren[0]);
        return topk;
    }
};

#endif //OLVP_TOPKNODE_HPP
