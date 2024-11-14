//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_TASKOUTPUTNODE_HPP
#define OLVP_TASKOUTPUTNODE_HPP



class TaskOutputNode :public PlanNode
{
    PlanNode *source;
public:

    TaskOutputNode (string id):PlanNode("TaskOutputNode",id){}

    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitTaskOutputNode(this,context);
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
        TaskOutputNode *output = new TaskOutputNode(this->getId());
        output->addSource(newChildren[0]);
        return output;
    }
    string getId()
    {
        return PlanNode::getId();
    }


};





#endif //OLVP_TASKOUTPUTNODE_HPP
