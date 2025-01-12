//
// Created by zxk on 12/29/24.
//

#ifndef OLVP_QUERY2JOIN_OC_PROBESHUFFLE_HPP
#define OLVP_QUERY2JOIN_OC_PROBESHUFFLE_HPP





#include "../../Query/RegQuery.h"

/*select count(o_orderkey) from orders,customer where orders.o_custkey=customer.c_custkey and customet.c_custkey < 30;*/

class Query2_Join_OC_ProbeShuffle:public RegQuery
{

public:
    Query2_Join_OC_ProbeShuffle(){

    }
    string getSql()  {return TpchSqls::Q2J_SmallOrders();}
    PlanNode* getPlanTree()
    {

        LookupJoinNode *join = createOrdersJoinCustomer();

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



    LookupJoinNode *createOrdersJoinCustomer()
    {


        PlanNode *tableScanOrders = this->createOrdersTableScan();
        PlanNode *tableScanCustomer = this->createCustomerTableScan();


        vector<FieldDesc> ordersProbeSchema = {FieldDesc("o_custkey","int64")};


        vector<FieldDesc> customerBuildSchema = {FieldDesc("c_custkey","int64")};

        vector<FieldDesc> customerBuildOutputSchema = {FieldDesc("c_custkey","int64")};


        vector<int> ordersprobeOutputChannels = {0};
        vector<int> ordersprobeHashChannels = {0};
        vector<int> customerbuildOutputChannels = {0};
        vector<int> customerbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(ordersProbeSchema,ordersprobeHashChannels,ordersprobeOutputChannels,customerBuildSchema,customerbuildHashChannels,customerbuildOutputChannels,customerBuildOutputSchema);
        LookupJoinNode *LOJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanCustomer);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanOrders);


        shared_ptr<PartitioningScheme> schemeProbe2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *probeExchange2 = new ExchangeNode("buildExchange",ExchangeNode::REPARTITION,schemeProbe2,probeExchange);



        LOJoin->addProbe(probeExchange2);
        LOJoin->addBuild(buildExchange);


        return LOJoin;


    }



    PlanNode *createOrdersTableScan()
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



    PlanNode *createCustomerTableScan()
    {
        //and o_orderdate < date '[DATE]'

        TableScanNode *tableScanCustomer = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","customer"));

        Column *c_custkey = new Column("0","c_custkey","int64");
        Int64Literal *int64Literal = new Int64Literal("0","50");


        FunctionCall *c_mktsegmentEqual = new FunctionCall("0","less_than","bool");
        c_mktsegmentEqual->addChilds({c_custkey,int64Literal});


        vector<FieldDesc> fieldsIn = {FieldDesc("c_custkey","int64"),
                                      FieldDesc("c_name","string"),
                                      FieldDesc("c_address","string"),
                                      FieldDesc("c_nationkey","int64"),
                                      FieldDesc("c_phone","string"),
                                      FieldDesc("c_acctbal","double"),
                                      FieldDesc("c_mktsegment","string"),
                                      FieldDesc("c_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,c_mktsegmentEqual);
        FilterNode *filterCustomer = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterCustomer->addSource(tableScanCustomer);



        ProjectAssignments assignments;
        assignments.addAssignment(FieldDesc("c_custkey","int64"),FieldDesc("c_custkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("c_name","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("c_address","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("c_nationkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("c_phone","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("c_acctbal","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("c_mktsegment","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("c_comment","string"),FieldDesc::getEmptyDesc(),NULL);

        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);


        projectNode->addSource(filterCustomer);



        return projectNode;


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







#endif //OLVP_QUERY2JOIN_OC_PROBESHUFFLE_HPP
