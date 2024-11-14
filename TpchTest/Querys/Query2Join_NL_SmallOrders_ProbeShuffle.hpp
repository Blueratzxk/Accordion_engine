//
// Created by zxk on 7/27/24.
//

#ifndef OLVP_QUERY2JOIN_NL_SMALLORDERS_PROBESHUFFLE_HPP
#define OLVP_QUERY2JOIN_NL_SMALLORDERS_PROBESHUFFLE_HPP


#include "../../Query/RegQuery.h"

/*select count(*) from lineitem,orders where lineitem.l_orderkey=orders.o_orderkey and orders.orderkey < 30;*/

class Query2_Join_NL_SmallOrders_ProbeShuffle:public RegQuery
{

public:
    Query2_Join_NL_SmallOrders_ProbeShuffle(){

    }
    string getSql()  {return TpchSqls::Q2J_SmallOrders();}
    PlanNode* getPlanTree()
    {

        LookupJoinNode *join = createLineitemJoinOrders();

        PartialAggregationNode *pagg = createPartialAgg();
        pagg->addSource(join);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(pagg);


        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));

        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,pagg);


        FinalAggregationNode *fagg = createFinalAgg();
        fagg->addSource(exchange);


        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(fagg);
        return (PlanNode*)output;
    }



    LookupJoinNode *createLineitemJoinOrders()
    {


        PlanNode *tableScanLineitem = this->createLineitemTableScan();
        PlanNode *tableScanOrders = this->createOrdersTableScan();


        vector<FieldDesc> lineitemProbeSchema = {FieldDesc("l_orderkey","int64")};


        vector<FieldDesc> ordersBuildSchema = {FieldDesc("o_orderkey","int64")};

        vector<FieldDesc> ordersBuildOutputSchema = {FieldDesc("o_orderkey","int64")};


        vector<int> lineitemprobeOutputChannels = {0};
        vector<int> lineitemprobeHashChannels = {0};
        vector<int> ordersbuildOutputChannels = {0};
        vector<int> ordersbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(lineitemProbeSchema,lineitemprobeHashChannels,lineitemprobeOutputChannels,ordersBuildSchema,ordersbuildHashChannels,ordersbuildOutputChannels,ordersBuildOutputSchema);
        LookupJoinNode *LOJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("buildExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);

        shared_ptr<PartitioningScheme> schemeProbe2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *probeExchange2 = new ExchangeNode("buildExchange",ExchangeNode::REPARTITION,schemeProbe2,probeExchange);


        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanOrders);

  //      shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
    //    ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);



        LOJoin->addProbe(probeExchange2);
        LOJoin->addBuild(buildExchange);


        return LOJoin;


    }



    PlanNode *createOrdersTableScan()
    {
        //and o_orderdate < date '[DATE]'

        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));



        Column *r_name = new Column("0","o_orderkey","int64");
        Int64Literal *rKeyLiteral = new Int64Literal("0","50");
        FunctionCall *rKeyLess = new FunctionCall("0","less_than","bool");
        rKeyLess->addChilds({r_name,rKeyLiteral});

        vector<FieldDesc> fieldsIn = {FieldDesc("o_orderkey","int64"),
                                      FieldDesc("o_custkey","int64"),
                                      FieldDesc("o_orderstatus","string"),
                                      FieldDesc("o_totalprice","double"),
                                      FieldDesc("o_orderdate","date32"),
                                      FieldDesc("o_orderpriority","string"),
                                      FieldDesc("o_clerk","string"),
                                      FieldDesc("o_shippriority","int64"),
                                      FieldDesc("o_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,rKeyLess);
        FilterNode *filterOrders = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterOrders->addSource(tableScanOrders);





        return filterOrders;

    }





    TableScanNode *createLineitemTableScan()
    {
        //l_commitdate < l_receiptdate
        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));
        return tableScanLineitem;
    }


    PartialAggregationNode *createPartialAgg()
    {


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"count_all",/*inputKey=*/"",/*outputName=*/"pcount_orderkey")},
                /*groupByKeys=*/{});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {


        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"pcount_orderkey",/*outputName=*/"fcount_orderkey")},
                /*groupByKeys=*/{});



        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }


};




#endif //OLVP_QUERY2JOIN_NL_SMALLORDERS_PROBESHUFFLE_HPP
