//
// Created by zxk on 5/15/24.
//

#ifndef OLVP_QUERY2JOIN_NL_HPP
#define OLVP_QUERY2JOIN_NL_HPP



#include "../../Query/RegQuery.h"

/*
 * select count(*) from lineitem,orders where lineitem.l_orderkey=orders.o_orderkey;
 *
 * */


class Query2_Join_NL:public RegQuery
{

public:
    Query2_Join_NL(){

    }
    string getSql()  {return TpchSqls::Q2J();}
    PlanNode* getPlanTree()
    {

        LookupJoinNode *join = createLineitemJoinOrders();

        PartialAggregationNode *pagg = createPartialAgg();
        pagg->addSource(join);


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


        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanOrders);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);



        LOJoin->addProbe(probeExchange);
        LOJoin->addBuild(buildExchange);


        return LOJoin;


    }



    ProjectNode *createOrdersTableScan()
    {
        //and o_orderdate < date '[DATE]'

        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        ProjectAssignments assignments;
        assignments.addAssignment(FieldDesc("o_orderkey","int64"),FieldDesc("o_orderkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("o_custkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderstatus","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_totalprice","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderpriority","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_clerk","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_shippriority","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_comment","string"),FieldDesc::getEmptyDesc(),NULL);
        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);


        projectNode->addSource(tableScanOrders);


        return projectNode;

    }





    ProjectNode *createLineitemTableScan()
    {
        //l_commitdate < l_receiptdate
        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));

        ProjectAssignments assignments;
        assignments.addAssignment(FieldDesc("l_orderkey","int64"),FieldDesc("l_orderkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("l_partkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_suppkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_linenumber","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_quantity","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_tax","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_returnflag","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_linestatus","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_shipdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_commitdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_receiptdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_shipinstruct","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_shipmode","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment( FieldDesc("l_comment","string"),FieldDesc::getEmptyDesc(),NULL);
        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);

        projectNode->addSource(tableScanLineitem);

        return projectNode;
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






#endif //OLVP_QUERY2JOIN_NL_HPP
