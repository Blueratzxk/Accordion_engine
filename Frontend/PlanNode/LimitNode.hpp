//
// Created by zxk on 9/23/23.
//

#ifndef OLVP_LIMITNODE_HPP
#define OLVP_LIMITNODE_HPP


#include "PlanNode.hpp"


class LimitNode :public PlanNode
{
    PlanNode *source;
    int limit;

public:


    LimitNode(string id,int limit):PlanNode("LimitNode",id)
    {
        this->limit = limit;
    }


    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitLimitNode(this,context);
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
    int getLimit()
    {
        return this->limit;
    }

    PlanNode* replaceChildren(vector<PlanNode*> newChildren){
        LimitNode *limitN = new LimitNode(this->getId(),this->limit);
        limitN->addSource(newChildren[0]);
        return limitN;
    }



};





#endif //OLVP_LIMITNODE_HPP
