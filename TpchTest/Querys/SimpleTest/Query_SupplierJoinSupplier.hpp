//
// Created by zxk on 6/29/24.
//

#ifndef OLVP_QUERY_SUPPLIERJOINSUPPLIER_HPP
#define OLVP_QUERY_SUPPLIERJOINSUPPLIER_HPP




#include "../../../Query/RegQuery.h"

/*
 * select count(*) from lineitem,orders where lineitem.l_orderkey=orders.o_orderkey;
 *
 * */


class Query_SupplierJoinSupplier:public RegQuery
{

public:
    Query_SupplierJoinSupplier(){

    }
    string getSql()  {return TpchSqls::QSJS();}
    PlanNode* getPlanTree()
    {

        LookupJoinNode *join = createLineitemJoinOrders();

        PartialAggregationNode *pagg = createPartialAgg();
        pagg->addSource(join);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(pagg);


        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));

        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,join);


        FinalAggregationNode *fagg = createFinalAgg();
        fagg->addSource(exchange);


        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(exchange);
        return (PlanNode*)output;
    }



    LookupJoinNode *createLineitemJoinOrders()
    {


        PlanNode *tableScanSupplier1 = this->createSupplierTableScan();
        PlanNode *tableScanSupplier2 = this->createSupplierTableScan();


        vector<FieldDesc> lineitemProbeSchema = {FieldDesc("n_nationkey","int64")};


        vector<FieldDesc> ordersBuildSchema = {FieldDesc("n_nationkey","int64")};

        vector<FieldDesc> ordersBuildOutputSchema = {FieldDesc("n_nationkey","int64")};


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


        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanSupplier1);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanSupplier2);

        LocalExchangeNode *taleScanLocalExchange = new LocalExchangeNode("taleScanLocalExchange");
        taleScanLocalExchange->addSource(probeExchange);

        LocalExchangeNode *hashexchange = new LocalExchangeNode("LocalExchange","hash",{0});
        hashexchange->addSource(buildExchange);

        LOJoin->addProbe(probeExchange);
        LOJoin->addBuild(hashexchange);


        return LOJoin;


    }





    TableScanNode *createSupplierTableScan()
    {
        //l_commitdate < l_receiptdate
        TableScanNode *tableScanSupplier = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","nation"));
        return tableScanSupplier;
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





#endif //OLVP_QUERY_SUPPLIERJOINSUPPLIER_HPP
