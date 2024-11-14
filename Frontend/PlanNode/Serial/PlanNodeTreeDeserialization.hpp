//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PLANNODETREEDESERIALIZATION_HPP
#define OLVP_PLANNODETREEDESERIALIZATION_HPP

#include "nlohmann/json.hpp"
#include "nlohmann/fifo_map.hpp"

using namespace nlohmann;

template<class K, class V, class dummy_compare, class A>
using my_workaround_fifo_map = fifo_map<K, V, fifo_map_compare<K>, A>;
using order_json = basic_json<my_workaround_fifo_map>;






class PlanNodeDeserializer
{
    typedef PlanNode*  (PlanNodeDeserializer::*Fun_ptr)(json node);
    map<string, Fun_ptr> funcMap;


public:
    PlanNodeDeserializer(){
        initFuctions();
    }

    void initFuctions()
    {
        funcMap.insert(make_pair("ExchangeNode", &PlanNodeDeserializer::getExchangeNode));
        funcMap.insert(make_pair("FilterNode", &PlanNodeDeserializer::getFilterNode));
        funcMap.insert(make_pair("FinalAggregationNode", &PlanNodeDeserializer::getFinalAggregationNode));
        funcMap.insert(make_pair("LookupJoinNode", &PlanNodeDeserializer::getLookupJoinNode));
        funcMap.insert(make_pair("CrossJoinNode", &PlanNodeDeserializer::getCrossJoinNode));
        funcMap.insert(make_pair("SortNode", &PlanNodeDeserializer::getSortNode));
        funcMap.insert(make_pair("TaskOutputNode", &PlanNodeDeserializer::getTaskOutputNode));
        funcMap.insert(make_pair("PartialAggregationNode", &PlanNodeDeserializer::getPartialAggregationNode));
        funcMap.insert(make_pair("ProjectNode", &PlanNodeDeserializer::getProjectNode));
        funcMap.insert(make_pair("RemoteSourceNode", &PlanNodeDeserializer::getRemoteSourceNode));
        funcMap.insert(make_pair("TableScanNode", &PlanNodeDeserializer::getTablescanNode));
        funcMap.insert(make_pair("TopKNode", &PlanNodeDeserializer::getTopKNode));
        funcMap.insert(make_pair("LocalExchangeNode", &PlanNodeDeserializer::getLocalExchangeNode));
        funcMap.insert(make_pair("LimitNode", &PlanNodeDeserializer::getLimitNode));

    }


    PlanNode *deserialize(string nodeName,json node)
    {

        if (funcMap.count(nodeName))
        {
            return  (this->*funcMap[nodeName])(node);
        }
        else
        {
            cout << "nodeDeserializer cannot find the node "<<nodeName << "!"<<endl;
            exit(0);
        }
    }

    PlanNode* getExchangeNode(json node) {

        PartitioningScheme ps;
        return new ExchangeNode(node["id"],node["exchangeType"],ps.Deserialize(node["patitioningScheme"]));

    };
    PlanNode* getFilterNode(json node) {

        string id = node["id"];
        return new FilterNode(id,FilterDescriptor::Deserialize(node["filterDescriptor"]));

    };

    PlanNode* getFinalAggregationNode(json node){

        return new FinalAggregationNode(node["id"],AggregationDesc::Deserialize(node["aggregationDesc"]));

    };

    PlanNode* getAggregationNode(json node){

        return new AggregationNode(node["id"],AggregationDesc::Deserialize(node["aggregationDesc"]));

    };

    PlanNode* getLookupJoinNode(json node) {

        return new LookupJoinNode(node["id"],LookupJoinDescriptor::Deserialize(node["lookupJoinDescriptor"]));
    };

    PlanNode* getCrossJoinNode(json node) {

        return new CrossJoinNode(node["id"],CrossJoinDescriptor::Deserialize(node["crossJoinDescriptor"]));
    };

    PlanNode* getSortNode(json node) {

        return new SortNode(node["id"],SortDescriptor::Deserialize(node["sortDescriptor"]));

    };
    PlanNode* getTaskOutputNode(json node){

        return new TaskOutputNode(node["id"]);
    };

    PlanNode* getPartialAggregationNode(json node){

        return new PartialAggregationNode(node["id"],AggregationDesc::Deserialize(node["aggregationDesc"]));
    };

    PlanNode* getProjectNode(json node) {

        return new ProjectNode(node["id"],ProjectAssignments::Deserialize(node["projectAssignments"]));

    };
    PlanNode* getLimitNode(json node) {

        return new LimitNode(node["id"],node["limit"]);
    };

    PlanNode* getRemoteSourceNode(json node) {

        return new RemoteSourceNode(node["id"]);
    };
    PlanNode* getTablescanNode(json node) {

        return new TableScanNode(node["id"],TableScanDescriptor::Deserialize(node["tableScanDescriptor"]));
    };
    PlanNode* getTopKNode(json node) {

        return new TopKNode(node["id"],TopKDescriptor::Deserialize(node["topKDescriptor"]));

    };
    PlanNode* getLocalExchangeNode(json node) {

        return new LocalExchangeNode(node["id"],node["exchangeType"],node["hashExchangeChannels"]);
    };


};



class PlanNodeTreeDeserializer
{
    PlanNodeDeserializer nodeDes;
public:

    PlanNodeTreeDeserializer(){

    }

    PlanNode* deserializeNode(string nodeName,json nodeJson)
    {
        return nodeDes.deserialize(nodeName,nodeJson);
    }
    PlanNode* deserializeSource(json sourceJson)
    {
        if(sourceJson.size() > 3 || sourceJson.size() <= 0){cout << "json tree error!";exit(0);}

        PlanNode *node = NULL;
        vector<PlanNode*> sourceNodes;

        for(auto elem : sourceJson.items()){

            if(elem.key().compare("source") == 0) {
                if(!elem.value().is_array()) {

                    sourceNodes.push_back(deserializeSource(elem.value()));
                }
                else
                {

                    json jsonArray = elem.value();
                    for(int i = 0 ; i < jsonArray.size() ; i++)
                    {
                        sourceNodes.push_back(deserializeSource(jsonArray[i]));
                    }
                }
            }
            else
            { //cout << elem.key() << endl;
                node = deserializeNode(elem.key(),elem.value());
            }
        }

        if(sourceNodes.size() > 1)
            node->addSources(sourceNodes);
        else if(sourceNodes.size() == 1)
            node->addSource(sourceNodes[0]);

        return node;

    }




    PlanNode* Deserialize(string plantree)
    {
        json treeJson = nlohmann::json::parse(plantree);
        PlanNode *root  = deserializeSource(treeJson);

        return root;

    }

};




#endif //OLVP_PLANNODETREEDESERIALIZATION_HPP
