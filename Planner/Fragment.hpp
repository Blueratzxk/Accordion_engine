//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_FRAGMENT_HPP
#define OLVP_FRAGMENT_HPP


#include "../Frontend/PlanNode/PlanNodePtr.hpp"
#include "../Frontend/PlanNode/Serial/PlanNodeTreeDeserialization.hpp"
#include "../Frontend/PlanNode/Serial/PlanNodeTreeSerialization.hpp"
#include "../Partitioning/PartitioningScheme.hpp"
#include "../Frontend/PlanNode/PlanNode.hpp"
#include "../Partitioning/PartitioningHandleSerializer.hpp"


class PlanFragment
{
    PlanNode* root;
    string fragmentId;
    shared_ptr<PartitioningScheme> partitioning_Scheme;
    shared_ptr<PartitioningHandle> handle;


public:
    PlanFragment(PlanNode* root){this->root = root;}
    PlanFragment(PlanNode* root,string fragmentId){this->root = root,this->fragmentId = fragmentId;}
    PlanFragment(){}

    PlanFragment(PlanNode* root,string fragmentId,shared_ptr<PartitioningScheme> partitioning_Scheme,shared_ptr<PartitioningHandle> handle){
        this->root = root;
        this->fragmentId = fragmentId;
        this->partitioning_Scheme = partitioning_Scheme;
        this->handle = handle;

    }

    PlanNode* getRoot()
    {
        return root;
    }
    string getFragmentId()
    {
        return this->fragmentId;
    }
    void setFragmentId(string id)
    {
        this->fragmentId = id;
    }

    bool hasTableScanId(string tableScanId)
    {
        return findTableScanIdOnSubPlan(this->root,tableScanId);
    }

    bool findTableScanIdOnSubPlan(PlanNode *root,string tableScanId) {

        if(root->getId() == tableScanId)
            return true;

        for (auto child: root->getSources()) {
            if(findTableScanIdOnSubPlan(child,tableScanId))
                return true;
        }
        return false;
    }

    void findRemoteSourceNode(PlanNode *node,vector<PlanNode *> &builder)
    {
        for (PlanNode* source : node->getSources()) {
            findRemoteSourceNode(source, builder);
        }
        if (node->getType().compare("RemoteSourceNode") == 0) {
            builder.push_back(node);
        }
    }

    void findTableScanNode(PlanNode *node,vector<PlanNode *> &builder)
    {
        for (PlanNode* source : node->getSources()) {
            findTableScanNode(source, builder);
        }
        if (node->getType().compare("TableScanNode") == 0) {
            builder.push_back(node);
        }
    }

    vector<PlanNode*> getRemoteSourceNodes()
    {
        vector<PlanNode*> remoteSourceNodes;
        this->findRemoteSourceNode(this->root,remoteSourceNodes);
        return remoteSourceNodes;
    }
    vector<PlanNode*> getTableScanNodes()
    {
        vector<PlanNode*> tableScanNodes;
        this->findTableScanNode(this->root,tableScanNodes);
        return tableScanNodes;
    }

    bool hasNodeType(string nodeType)
    {
        return findNodeType(this->root,nodeType);
    }
    bool findNodeType(PlanNode *node,string nodeType)
    {
        if(node->getType() == nodeType)
            return true;

        for(auto source : node->getSources()) {
            if(findNodeType(source, nodeType))
                return true;
        }
        return false;
    }

    bool hasTableScan()
    {
        return this->getTableScanNodes().size() > 0;
    }
    shared_ptr<PartitioningScheme> getPartitionScheme()
    {
        return this->partitioning_Scheme;
    }
    shared_ptr<PartitioningHandle> getPartitionHandle()
    {
        return this->handle;
    }


    static string Serialize(PlanFragment planFragment)
    {
        nlohmann::json json;
        PlanNodeTreeSerializer serializer;
        json["root"] = serializer.Serialize(planFragment.root);
        json["fragmentId"] = planFragment.fragmentId;

        if(planFragment.partitioning_Scheme != NULL)
            json["partitioning_Scheme"] = planFragment.partitioning_Scheme->Serialize();
        else
            json["partitioning_Scheme"] = "NULL";

        json["handle"] = PartitioningHandleSerializer::Serialize((planFragment.getPartitionHandle()));

        string result = json.dump();
        return result;
    }
    static shared_ptr<PlanFragment> Deserialize(string planFragment)
    {
        if(planFragment == "NULL")
            return NULL;

        nlohmann::json json = nlohmann::json::parse(planFragment);

        PlanNodeTreeDeserializer deserializer;
        PartitioningScheme partitioningScheme;
        return make_shared<PlanFragment>(deserializer.Deserialize(json["root"]),json["fragmentId"],partitioningScheme.Deserialize(json["partitioning_Scheme"]),
                                         PartitioningHandleSerializer::Deserialize(json["handle"]));
    }



};

class SubPlan:public enable_shared_from_this<SubPlan>
{
    PlanFragment fragment;
    vector<std::shared_ptr<SubPlan>> children;
public:
    SubPlan(PlanFragment fragment,vector<std::shared_ptr<SubPlan>> children)
    {
        this->fragment = fragment;
        for(int i = 0 ; i < children.size() ; i++)
            this->children.push_back(children[i]);
    }
    PlanFragment getFragment()
    {
        return this->fragment;
    }
    void setFragmentId(string id)
    {
        this->fragment.setFragmentId(id);
    }
    vector<std::shared_ptr<SubPlan>> getChildren()
    {
        return this->children;
    }
    vector<PlanFragment> getAllFragments()
    {
        vector<PlanFragment> fragments;
        fragments.push_back(getFragment());
        for(int  i = 0 ; i < getChildren().size() ; i++)
        {
            vector<PlanFragment> frags = getChildren()[i]->getAllFragments();
            fragments.insert(fragments.end(),frags.begin(),frags.end());
        }

        return fragments;
    }
    void add(std::shared_ptr<SubPlan> plan,int *id)
    {
        plan->fragment.setFragmentId(to_string(*id));
        (*id)++;

        for(int i = 0 ; i < plan->getChildren().size() ; i++)
        {
            add(plan->getChildren()[i],id);
        }
    }
    void addFragmentIds()
    {
        int i = 0;
        add(shared_from_this(),&i);
    }

    ~SubPlan()
    {
        PlanNodePtr Ptr(this->getFragment().getRoot());
    }


};

class FragmentProperties
{
    vector<std::shared_ptr<SubPlan>> childrens;
    int *nextFragmentId;
    shared_ptr<PartitioningScheme> partitioning_Scheme;

    std::optional<shared_ptr<PartitioningHandle>> handle =  std::nullopt;

    vector<PlanNode *> partitionedSources; //tableScan source maybe distributed storage

public:
    FragmentProperties(){}

    FragmentProperties(shared_ptr<PartitioningScheme> partitioning_Scheme,int *nextId){
        this->nextFragmentId = nextId;
        this->partitioning_Scheme = partitioning_Scheme;
    }

    FragmentProperties(int *nextId){this->nextFragmentId = nextId;}

    vector<std::shared_ptr<SubPlan>> getChildren(){return this->childrens;}

    void addChildren(vector<std::shared_ptr<SubPlan>> children)
    {
        if(children.size() > 0)
            this->childrens.insert(this->childrens.end(),children.begin(),children.end());
    }
    void addChildren(std::shared_ptr<SubPlan> children)
    {
        this->childrens.push_back(children);
    }


    int *getIdAddr()
    {
        return this->nextFragmentId;
    }

    string getNextFragmentId() {
        (*nextFragmentId)++;
        return to_string(*nextFragmentId);
    }


    void setSingleNodeDistribution()
    {
        if(handle.has_value() && handle.value()->isSingleNode())
        {
            return;
        }
        handle = SystemPartitioningHandle::SINGLE_DISTRIBUTION;
    }
    void setDistribution(shared_ptr<PartitioningHandle> distribution)
    {
        handle = distribution;
    }

    shared_ptr<PartitioningScheme> getPartitioningScheme()
    {
        return this->partitioning_Scheme;
    }
    std::optional<shared_ptr<PartitioningHandle>> getHandle()
    {
        shared_ptr<PartitioningHandle> handle1 = NULL;
        if(this->handle.has_value())
            return this->handle;
        else
            return handle1;

    }


};





#endif //OLVP_FRAGMENT_HPP
