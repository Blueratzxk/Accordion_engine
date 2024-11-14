//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_EXCHANGENODE_HPP
#define OLVP_EXCHANGENODE_HPP

#include "../../Partitioning/PartitioningScheme.hpp"
#include "../../Partitioning/SystemPartitioningHandle.h"
class ExchangeNode: public PlanNode
{
public:
    enum Type
    {
        GATHER,
        REPARTITION,
        REPLICATE
    };

private:
    PlanNode *source;
    shared_ptr<PartitioningScheme> partitioning_Scheme;
    Type type;

public:

    static ExchangeNode *getGatheringExchangeNode(string id,PlanNode *child)
    {
        shared_ptr<Partitioning> par = Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{});
        vector<int> bucketToPartition;
        auto  scheme = make_shared<PartitioningScheme>(par,bucketToPartition);
        return new ExchangeNode(id,GATHER,scheme,child);
    }

    static ExchangeNode *getReplicatedExchangeNode(string id,PlanNode *child)
    {
        shared_ptr<Partitioning> par = Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{});
        vector<int> bucketToPartition;
        auto  scheme = make_shared<PartitioningScheme>(par,bucketToPartition);
        return new ExchangeNode(id,REPLICATE,scheme,child);
    }

    static ExchangeNode *getRoundRobinExchangeNode(string id,PlanNode *child)
    {
        shared_ptr<Partitioning> par  = Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{});
        vector<int> bucketToPartition;
        auto  scheme = make_shared<PartitioningScheme>(par,bucketToPartition);
        return new ExchangeNode(id,REPARTITION,scheme,child);
    }

    Type getExchangeType()
    {
        return this->type;
    }


    ExchangeNode(string id,ExchangeNode::Type type,shared_ptr<PartitioningScheme> scheme):PlanNode("ExchangeNode",id)
    {
        this->type = type;
        this->partitioning_Scheme = scheme;
    }
    ExchangeNode(string id,ExchangeNode::Type type,shared_ptr<PartitioningScheme> scheme,PlanNode *source):PlanNode("ExchangeNode",id)
    {
        this->type = type;
        this->partitioning_Scheme = scheme;
        this->source = source;
    }

    void* accept(NodeVisitor* visitor,void *context)  {
        return visitor->VisitExchangeNode(this,context);
    }
    void addSource(PlanNode *node)
    {
        this->source = node;
    }

    vector<shared_ptr<VariableReferenceExpression>> getOutputVariables() override
    {
        return this->source->getOutputVariables();
    }

    shared_ptr<PartitioningScheme> getPartitioningScheme()
    {
        return this->partitioning_Scheme;
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
        ExchangeNode *exchange = new ExchangeNode(this->getId(),this->type,this->getPartitioningScheme(),this->source);
        exchange->addSource(newChildren[0]);
        return exchange;
    }

};

#endif //OLVP_EXCHANGENODE_HPP
