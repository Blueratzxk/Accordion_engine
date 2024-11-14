//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PLANNODETREESERIALIZATION_HPP
#define OLVP_PLANNODETREESERIALIZATION_HPP


#include "nlohmann/json.hpp"
#include "nlohmann/fifo_map.hpp"
using namespace nlohmann;

template<class K, class V, class dummy_compare, class A>
using my_workaround_fifo_map = fifo_map<K, V, fifo_map_compare<K>, A>;
using order_json = basic_json<my_workaround_fifo_map>;



class PlanNodeTreeSerializer : public NodeVisitor
{
public:
    PlanNodeTreeSerializer(){}


    void* Visit(PlanNode* node,void *context) {
        return node->accept(this,context);
    }
    void* VisitExchangeNode(ExchangeNode* node,void *context) {

        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;
        order_json *Exchange = new order_json();
        PlanNodeAttributes["id"] = node->getId();
        PlanNodeAttributes["patitioningScheme"] = node->getPartitioningScheme()->Serialize();
        PlanNodeAttributes["exchangeType"] = node->getExchangeType();

        (*Exchange)["ExchangeNode"] = PlanNodeAttributes;




        (*Exchange)["source"] = *child;
        delete(child);
        return Exchange;

    }
    void* VisitFilterNode(FilterNode* node,void *context) {

        order_json* child = (order_json*)Visit(node->getSource(),context);



        order_json PlanNodeAttributes;

        PlanNodeAttributes["filterDescriptor"] = FilterDescriptor::Serialize(node->getFilterDesc());
        PlanNodeAttributes["id"] = node->getId();
        order_json *Filter = new order_json();
        (*Filter)["FilterNode"] = PlanNodeAttributes;


        (*Filter)["source"] = *child;

        delete(child);
        return Filter;

    }

    void* VisitFinalAggregationNode(FinalAggregationNode* node,void *context) {

        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;
        PlanNodeAttributes["aggregationDesc"] = AggregationDesc::Serialize(node->getAggregationDesc());
        PlanNodeAttributes["id"] = node->getId();


        order_json *FinalAgg = new order_json();
        (*FinalAgg)["FinalAggregationNode"] = PlanNodeAttributes;
        (*FinalAgg)["source"] = *child;


        delete(child);
        return FinalAgg;

    }
    void* VisitLookupJoinNode(LookupJoinNode* node,void *context) {


        order_json* probe = (order_json*)Visit(node->getProbe(),context);
        order_json* build = (order_json*)Visit(node->getBuild(),context);



        order_json PlanNodeAttributes;

        PlanNodeAttributes["lookupJoinDescriptor"] = LookupJoinDescriptor::Serialize(node->getLookupJoinDescriptor());
        PlanNodeAttributes["id"] = node->getId();
        order_json *LookupJoin = new order_json();

        (*LookupJoin)["LookupJoinNode"] = PlanNodeAttributes;


        order_json sources = order_json::array();
        sources.push_back(*probe);
        sources.push_back(*build);
        (*LookupJoin)["source"] = sources;

        delete(probe);
        delete(build);
        return LookupJoin;

    }

    void* VisitCrossJoinNode(CrossJoinNode* node,void *context) {


        order_json* probe = (order_json*)Visit(node->getProbe(),context);
        order_json* build = (order_json*)Visit(node->getBuild(),context);



        order_json PlanNodeAttributes;

        PlanNodeAttributes["crossJoinDescriptor"] = CrossJoinDescriptor::Serialize(node->getCrossJoinDescriptor());
        PlanNodeAttributes["id"] = node->getId();
        order_json *LookupJoin = new order_json();

        (*LookupJoin)["CrossJoinNode"] = PlanNodeAttributes;


        order_json sources = order_json::array();
        sources.push_back(*probe);
        sources.push_back(*build);
        (*LookupJoin)["source"] = sources;

        delete(probe);
        delete(build);
        return LookupJoin;

    }
    void* VisitSortNode(SortNode* node,void *context) {


        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;

        PlanNodeAttributes["sortDescriptor"] = SortDescriptor::Serialize(node->getSortDesc());
        PlanNodeAttributes["id"] = node->getId();
        order_json *Sort = new order_json();
        (*Sort)["SortNode"] = PlanNodeAttributes;


        (*Sort)["source"] = *child;

        delete(child);
        return Sort;

    }
    void* VisitTaskOutputNode(TaskOutputNode* node,void *context) {

        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;
        PlanNodeAttributes["id"] = node->getId();

        order_json *TaskOutput = new order_json();
        (*TaskOutput)["TaskOutputNode"] = PlanNodeAttributes;


        (*TaskOutput)["source"] = *child;

        delete(child);
        return TaskOutput;
    }
    void* VisitPartialAggregationNode(PartialAggregationNode* node,void *context) {

        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;
        PlanNodeAttributes["aggregationDesc"] = AggregationDesc::Serialize(node->getAggregationDesc());
        PlanNodeAttributes["id"] = node->getId();


        order_json *Pagg = new order_json();
        (*Pagg)["PartialAggregationNode"] = PlanNodeAttributes;


        (*Pagg)["source"] = *child;

        delete(child);
        return Pagg;
    }

    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;
     //   PlanNodeAttributes["aggregationDesc"] = AggregationDesc::Serialize(node->getAggregationDesc());
        PlanNodeAttributes["id"] = node->getId();


        order_json *Pagg = new order_json();
        (*Pagg)["AggregationNode"] = PlanNodeAttributes;


        (*Pagg)["source"] = *child;

        delete(child);
        return Pagg;
    }


    void* VisitProjectNode(ProjectNode* node,void *context) {


        order_json* child = (order_json*)Visit(node->getSource(),context);


        order_json PlanNodeAttributes;
        PlanNodeAttributes["id"] = node->getId();
        PlanNodeAttributes["projectAssignments"] = ProjectAssignments::Serialize(node->getProjectAssignments());

        order_json *Proj = new order_json();
        (*Proj)["ProjectNode"] = PlanNodeAttributes;


        (*Proj)["source"] = *child;

        delete(child);
        return Proj;


    }

    void* VisitLimitNode(LimitNode* node,void *context) {


        order_json* child = (order_json*)Visit(node->getSource(),context);


        order_json PlanNodeAttributes;
        PlanNodeAttributes["id"] = node->getId();
        PlanNodeAttributes["limit"] = node->getLimit();

        order_json *LimitN = new order_json();
        (*LimitN)["LimitNode"] = PlanNodeAttributes;


        (*LimitN)["source"] = *child;

        delete(child);
        return LimitN;


    }

    void* VisitRemoteSourceNode(RemoteSourceNode* node,void *context) {


        order_json PlanNodeAttributes;
        PlanNodeAttributes["id"] = node->getId();
        order_json *RemoteSource = new order_json();
        (*RemoteSource)["RemoteSourceNode"] = PlanNodeAttributes;



        return RemoteSource;

    }
    void* VisitTableScanNode(TableScanNode* node,void *context) {



        order_json PlanNodeAttributes;
        PlanNodeAttributes["id"] = node->getId();
        PlanNodeAttributes["tableScanDescriptor"] = TableScanDescriptor::Serialize(node->getDescriptor());


        order_json *TableScan = new order_json();
        (*TableScan)["TableScanNode"] = PlanNodeAttributes;



        return TableScan;

    }
    void* VisitTopKNode(TopKNode* node,void *context) {

        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;

        PlanNodeAttributes["id"] = node->getId();
        PlanNodeAttributes["topKDescriptor"] = TopKDescriptor::Serialize(node->getTopKDescriptor());


        order_json *topn = new order_json();
        (*topn)["TopKNode"] = PlanNodeAttributes;



        (*topn)["source"] = *child;

        delete(child);
        return topn;

    }
    void* VisitLocalExchangeNode(LocalExchangeNode* node,void *context) {

        order_json* child = (order_json*)Visit(node->getSource(),context);

        order_json PlanNodeAttributes;
        PlanNodeAttributes["id"] = node->getId();
        PlanNodeAttributes["exchangeType"] = node->getExchangeType();
        PlanNodeAttributes["hashExchangeChannels"] = node->gethashExchangeChannels();
        order_json *localExchange = new order_json();

        (*localExchange)["LocalExchangeNode"] = PlanNodeAttributes;



        (*localExchange)["source"] = *child;

        delete(child);
        return localExchange;

    }


    string Serialize(PlanNode *root)
    {
        order_json *output = (order_json*)this->Visit(root,NULL);
        string s = output->dump(2);

        delete(output);
        return s;
    }


    void test(){
        /*

        json j ;
        vector<int> se = {1213,3213,213213,2132};
        j["object"] = {se,{"3213",123}};

        json jj;
        jj["ddd"] = "222";
        j["dggg"] = jj;

        string s = j.dump();
        cout << s <<endl;

        filterNode fo({{predicate("stringLike", DOUBLEINDEX, "6", "0"), predicate("greaterEqualInt", LEFTINDEX, "0",
                                                                                  "200"), predicate("lessEqualInt",
                                                                                                    LEFTINDEX, "0",
                                                                                                    "550")}});

        json *vvv =  (json*)VisitFilterNode(&fo,NULL);
        string str = vvv->dump();
        cout << str;
          */
    }

};



#endif //OLVP_PLANNODETREESERIALIZATION_HPP
