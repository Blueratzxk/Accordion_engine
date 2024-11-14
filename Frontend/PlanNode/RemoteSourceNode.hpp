//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_REMOTESOURCENODE_HPP
#define OLVP_REMOTESOURCENODE_HPP

class RemoteSourceNode:public PlanNode
{

    string sourceFragmentId;
public:

    RemoteSourceNode(string id):PlanNode("RemoteSourceNode",id)
    {
    }
    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitRemoteSourceNode(this,context);
    }
    void addSource(PlanNode *node){}
    void addSources(PlanNode *node){}

    string getId()
    {
        return PlanNode::getId();
    }
    void setSourceFragmentId(string id)
    {
        this->sourceFragmentId = id;
    }
    string getSourceFragmentId()
    {
        return this->sourceFragmentId;
    }


    PlanNode* replaceChildren(vector<PlanNode*> newChildren){
        return new RemoteSourceNode(this->getId());
    }

};

#endif //OLVP_REMOTESOURCENODE_HPP
