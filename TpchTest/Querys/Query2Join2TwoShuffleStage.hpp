//
// Created by zxk on 6/20/24.
//

#ifndef OLVP_QUERY2JOIN2TWOSHUFFLESTAGE_HPP
#define OLVP_QUERY2JOIN2TWOSHUFFLESTAGE_HPP


#include "../../Query/RegQuery.h"
class Query2_Join2_TwoShuffleStage:public RegQuery
{

public:
    Query2_Join2_TwoShuffleStage(){

    }
    string getSql()  {return TpchSqls::Q2J2();}
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
        PlanNode *tableScanSupplier = this->createSupplierTableScan();


        vector<FieldDesc> lineitemProbeSchema = {FieldDesc("l_suppkey","int64")};


        vector<FieldDesc> supplierBuildSchema = {FieldDesc("l_suppkey","int64")};

        vector<FieldDesc> supplierBuildOutputSchema = {FieldDesc("l_suppkey","int64")};


        vector<int> lineitemprobeOutputChannels = {2};
        vector<int> lineitemprobeHashChannels = {2};
        vector<int> ordersbuildOutputChannels = {0};
        vector<int> ordersbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(lineitemProbeSchema,lineitemprobeHashChannels,lineitemprobeOutputChannels,supplierBuildSchema,ordersbuildHashChannels,ordersbuildOutputChannels,supplierBuildOutputSchema);
        LookupJoinNode *LOJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe222 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF"),{}));
        ExchangeNode *buildExchange222 = new ExchangeNode("buildExchange",ExchangeNode::REPARTITION,schemeProbe222,tableScanSupplier);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPARTITION,schemeBuild,buildExchange222);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("buildExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);

        shared_ptr<PartitioningScheme> schemeProbe2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *probeExchange2 = new ExchangeNode("buildExchange",ExchangeNode::REPARTITION,schemeProbe2,probeExchange);




        LOJoin->addProbe(probeExchange2);
        LOJoin->addBuild(buildExchange);


        return LOJoin;


    }



    TableScanNode *createOrdersTableScan()
    {


        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));
        return tableScanOrders;

    }

    TableScanNode *createSupplierTableScan()
    {


        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));
        return tableScanOrders;

    }



    TableScanNode *createLineitemTableScan()
    {
        //l_commitdate < l_receiptdate
        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));
        return tableScanLineitem;
    }


    PartialAggregationNode *createPartialAgg()
    {


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"count_all",/*inputKey=*/"",/*outputName=*/"pcount_suppkey")},
                /*groupByKeys=*/{});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {


        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"pcount_suppkey",/*outputName=*/"fcount_suppkey")},
                /*groupByKeys=*/{});



        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }


};



#endif //OLVP_QUERY2JOIN2TWOSHUFFLESTAGE_HPP
