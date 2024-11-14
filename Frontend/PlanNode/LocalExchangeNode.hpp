//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_LOCALEXCHANGENODE_HPP
#define OLVP_LOCALEXCHANGENODE_HPP


class LocalExchangeNode:public PlanNode
{

    PlanNode *source;

    string exchangeType;
    vector<int> hashExchangeChannels;

public:


    LocalExchangeNode(string id):PlanNode("LocalExchangeNode",id)
    {
        exchangeType = "Arbitrary";
    }
    string getExchangeType()
    {
        return this->exchangeType;
    }
    vector<int> gethashExchangeChannels()
    {
        return this->hashExchangeChannels;
    }
    LocalExchangeNode(string id,string exchangeType,vector<int> hashExchangeChannels):PlanNode("LocalExchangeNode",id)
    {
        this->exchangeType = exchangeType;
        this->hashExchangeChannels = hashExchangeChannels;
    }

    void* accept(NodeVisitor* visitor,void *context)  {
        return visitor->VisitLocalExchangeNode(this,context);
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
        LocalExchangeNode *localExchangeNode = new LocalExchangeNode(this->getId(),this->getExchangeType(),this->gethashExchangeChannels());
        localExchangeNode->addSource(newChildren[0]);
        return localExchangeNode;
    }

};




#endif //OLVP_LOCALEXCHANGENODE_HPP
