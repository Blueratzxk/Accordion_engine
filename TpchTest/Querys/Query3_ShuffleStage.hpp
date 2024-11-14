//
// Created by zxk on 9/29/23.
//

#ifndef OLVP_QUERY3_SHUFFLESTAGE_HPP
#define OLVP_QUERY3_SHUFFLESTAGE_HPP


#include "../../Query/RegQuery.h"
class Query3_ShuffleStage:public RegQuery
{

public:
    Query3_ShuffleStage(){

    }
    string getSql()  {return TpchSqls::Q3();}
    PlanNode* getPlanTree()
    {


        PlanNode *threeTableJoin = create3TJ();




        PlanNode *proj = createProject();
        proj->addSource(threeTableJoin);

        PlanNode *partialAgg = createPartialAgg();
        partialAgg->addSource(proj);


        LocalExchangeNode *paggExchange = new LocalExchangeNode("paggExchange");
        paggExchange->addSource(partialAgg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,paggExchange);



        PlanNode *finalAgg = createFinalAgg();
        finalAgg->addSource(exchange);



        vector<string> sortKeys = {"revenue","o_orderdate"};
        vector<TopKOperator::SortOrder> sortOrders = {TopKOperator::Descending,TopKOperator::Ascending};

        TopKDescriptor topkDescriptor(100,sortKeys,sortOrders);
        TopKNode *topk = new TopKNode(UUID::create_uuid(),topkDescriptor);
        topk->addSource(finalAgg);




        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(topk);
        return (PlanNode*)output;
    }

    FilterNode *createOrdersTableScanAndFilter()
    {
        //and o_orderdate < date '[DATE]'

        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        Column *o_orderdate = new Column("0","o_orderdate","date32");
        Date32Literal *date32Literal = new Date32Literal("0","1995-03-21");

        FunctionCall *castDate = new FunctionCall("0","castDATE","date32");
        castDate->addChilds({date32Literal});


        FunctionCall *o_orderdate_Less_than = new FunctionCall("0","less_than","bool");
        o_orderdate_Less_than->addChilds({o_orderdate,castDate});


        vector<FieldDesc> fieldsIn = {FieldDesc("o_orderkey","int64"),
                                      FieldDesc("o_custkey","int64"),
                                      FieldDesc("o_orderstatus","string"),
                                      FieldDesc("o_totalprice","double"),
                                      FieldDesc("o_orderdate","date32"),
                                      FieldDesc("o_orderpriority","string"),
                                      FieldDesc("o_clerk","string"),
                                      FieldDesc("o_shippriority","int64"),
                                      FieldDesc("o_comment","string")};



        FilterDescriptor filterDescriptor(fieldsIn,o_orderdate_Less_than);
        FilterNode *filterOrders = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterOrders->addSource(tableScanOrders);
        return filterOrders;
    }

    FilterNode *createCustomerTableScanAndFilter()
    {


        TableScanNode *tableScanCustomer = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","customer"));

        Column *c_mktsegment = new Column("0","c_mktsegment","string");
        StringLiteral *stringLiteral = new StringLiteral("0","BUILDING");


        FunctionCall *c_mktsegmentEqual = new FunctionCall("0","equal","bool");
        c_mktsegmentEqual->addChilds({c_mktsegment,stringLiteral});


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
        return filterCustomer;


    }

    LookupJoinNode *createCustomerJoinOrders()
    {


        FilterNode *filteredCustomer = this->createCustomerTableScanAndFilter();
        FilterNode *filteredOrders = this->createOrdersTableScanAndFilter();




        vector<FieldDesc> fOrdersProbeSchema = {FieldDesc("o_orderkey","int64"),
                                                FieldDesc("o_custkey","int64"),
                                                FieldDesc("o_orderstatus","string"),
                                                FieldDesc("o_totalprice","double"),
                                                FieldDesc("o_orderdate","date32"),
                                                FieldDesc("o_orderpriority","string"),
                                                FieldDesc("o_clerk","string"),
                                                FieldDesc("o_shippriority","int64"),
                                                FieldDesc("o_comment","string")};

        vector<FieldDesc> fCustomerBuildSchema = {FieldDesc("c_custkey","int64"),
                                                  FieldDesc("c_name","string"),
                                                  FieldDesc("c_address","string"),
                                                  FieldDesc("c_nationkey","int64"),
                                                  FieldDesc("c_phone","string"),
                                                  FieldDesc("c_acctbal","double"),
                                                  FieldDesc("c_mktsegment","string"),
                                                  FieldDesc("c_comment","string")};



        vector<FieldDesc> fCustomerBuildOutputSchema = {FieldDesc("c_custkey","int64")};
        vector<int> fOrdersprobeOutputChannels = {0,4,7};
        vector<int> fOrdersprobeHashChannels = {1};
        vector<int> fCustomerbuildOutputChannels = {0};
        vector<int> fCustomerbuildHashChannels = {0};
        LookupJoinDescriptor fcfoLookupJoinDescriptor(fOrdersProbeSchema,fOrdersprobeHashChannels,fOrdersprobeOutputChannels,fCustomerBuildSchema,fCustomerbuildHashChannels,fCustomerbuildOutputChannels,fCustomerBuildOutputSchema);
        LookupJoinNode *customerOrdersJoin = new LookupJoinNode(UUID::create_uuid(),fcfoLookupJoinDescriptor);

        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"1"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,filteredOrders);

        shared_ptr<PartitioningScheme> schemeProbe22 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_SHUFFLE_STAGE_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange22 = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe22,probeExchange);


        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,filteredCustomer);


   //     shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
    //    ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,filteredOrders);


        LocalExchangeNode *localExchangeOrdersTableScan = new LocalExchangeNode("localExchangeOrdersTableScan");
        localExchangeOrdersTableScan->addSource(probeExchange22);

        customerOrdersJoin->addProbe(localExchangeOrdersTableScan);
        customerOrdersJoin->addBuild(buildExchange);


        return customerOrdersJoin;

    }

    FilterNode *createLineitemTableScanAndFilter()
    {

        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));

        Column *l_shipdate = new Column("0","l_shipdate","date32");

        Date32Literal *date32Literal = new Date32Literal("0","1995-03-21");
        FunctionCall *castDate = new FunctionCall("0","castDATE","date32");
        castDate->addChilds({date32Literal});


        FunctionCall *l_shipdate_greater = new FunctionCall("0","greater_than","bool");
        l_shipdate_greater->addChilds({l_shipdate,castDate});



        vector<FieldDesc> fieldsIn = {FieldDesc("l_orderkey","int64"),
                                      FieldDesc("l_partkey","int64"),
                                      FieldDesc("l_suppkey","int64"),
                                      FieldDesc("l_linenumber","int64"),
                                      FieldDesc("l_quantity","int64"),
                                      FieldDesc("l_extendedprice","double"),
                                      FieldDesc("l_discount","double"),
                                      FieldDesc("l_tax","double"),
                                      FieldDesc("l_returnflag","string"),
                                      FieldDesc("l_linestatus","string"),
                                      FieldDesc("l_shipdate","date32"),
                                      FieldDesc("l_commitdate","date32"),
                                      FieldDesc("l_receiptdate","date32"),
                                      FieldDesc("l_shipinstruct","string"),
                                      FieldDesc("l_shipmode","string"),
                                      FieldDesc("l_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,l_shipdate_greater);
        FilterNode *filterLineitem = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterLineitem->addSource(tableScanLineitem);
        return filterLineitem;

    }

    LookupJoinNode *create3TJ()
    {

        LookupJoinNode *customerJoinOrders = this->createCustomerJoinOrders();
        FilterNode *filteredLineitem = createLineitemTableScanAndFilter();




        vector<FieldDesc> fLineitemProbeSchema = {FieldDesc("l_orderkey","int64"),
                                                  FieldDesc("l_partkey","int64"),
                                                  FieldDesc("l_suppkey","int64"),
                                                  FieldDesc("l_linenumber","int64"),
                                                  FieldDesc("l_quantity","int64"),
                                                  FieldDesc("l_extendedprice","double"),
                                                  FieldDesc("l_discount","double"),
                                                  FieldDesc("l_tax","double"),
                                                  FieldDesc("l_returnflag","string"),
                                                  FieldDesc("l_linestatus","string"),
                                                  FieldDesc("l_shipdate","date32"),
                                                  FieldDesc("l_commitdate","date32"),
                                                  FieldDesc("l_receiptdate","date32"),
                                                  FieldDesc("l_shipinstruct","string"),
                                                  FieldDesc("l_shipmode","string"),
                                                  FieldDesc("l_comment","string")};

        vector<FieldDesc> customerJoinOrdersBuildSchema = {FieldDesc("o_orderkey","int64"),
                                                           FieldDesc("o_orderdate","date32"),
                                                           FieldDesc("o_shippriority","int64"),
                                                           FieldDesc("c_custkey","int64")};



        vector<FieldDesc> customerJoinOrdersBuildOutputSchema = {FieldDesc("o_orderdate","date32"),FieldDesc("o_shippriority","int64")};

        vector<int> fLineitemprobeOutputChannels = {0,5,6};
        vector<int> fLineitemprobeHashChannels = {0};
        vector<int> customerJoinOrdersbuildOutputChannels = {1,2};
        vector<int> customerJoinOrdersbuildHashChannels = {0};
        LookupJoinDescriptor threeTJ_LookupJoinDescriptor(fLineitemProbeSchema,fLineitemprobeHashChannels,fLineitemprobeOutputChannels,customerJoinOrdersBuildSchema,customerJoinOrdersbuildHashChannels,customerJoinOrdersbuildOutputChannels,customerJoinOrdersBuildOutputSchema);
        LookupJoinNode *customerOrdersJoin = new LookupJoinNode(UUID::create_uuid(),threeTJ_LookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;



        LocalExchangeNode *joinExchange = new LocalExchangeNode("joinExchange");
        joinExchange->addSource(customerJoinOrders);



        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,joinExchange);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,filteredLineitem);

        shared_ptr<PartitioningScheme> schemeProbe22 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_SHUFFLE_STAGE_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange22 = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe22,probeExchange);

      //  shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
     //   ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,filteredLineitem);

        LocalExchangeNode *filterlineitemExchange = new LocalExchangeNode("filterlineitemExchange");
        filterlineitemExchange->addSource(probeExchange22);


        customerOrdersJoin->addProbe(filterlineitemExchange);
        customerOrdersJoin->addBuild(buildExchange);

        return customerOrdersJoin;

    }



    ProjectNode * createProject()
    {


        /*
     l_orderkey|l_extendedprice|l_discount|o_orderdate|o_orderpriority|o_shippriority|
int64|double|double|date32[day]|string|int64|


      l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue, //潜在的收入，聚集操作
o_orderdate,
     o_shippriority
*/

        //l_extendedprice * (1 - l_discount) ==> disc_price

        DoubleLiteral *const1 = new DoubleLiteral("0","1");
        Column *col_l_discount = new Column("0","l_discount","double");

        FunctionCall *disc_price_sub = new FunctionCall("0","subtract","double");
        disc_price_sub->addChilds({const1,col_l_discount});


        Column *col_l_extendedprice = new Column("0","l_extendedprice","double");

        FunctionCall *disc_price = new FunctionCall("0","multiply","double");
        disc_price->addChilds({disc_price_sub,col_l_extendedprice});




        ProjectAssignments assignments;


        //l_extendedprice*(1-l_discount)
        assignments.addAssignment(FieldDesc("l_orderkey","int64"),FieldDesc("l_orderkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("oneOfRevenue","double"),disc_price);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderdate","date32"),FieldDesc("o_orderdate","date32"),NULL);
        assignments.addAssignment(FieldDesc("o_shippriority","int64"),FieldDesc("o_shippriority","int64"),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }

    PartialAggregationNode *createPartialAgg()
    {

        /*
        l_orderkey|l_extendedprice|l_discount|o_orderdate|o_orderpriority|o_shippriority|
   int64|double|double|date32[day]|string|int64|


         l_orderkey,
   sum(l_extendedprice*(1-l_discount)) as revenue, //潜在的收入，聚集操作
   o_orderdate,
        o_shippriority
   ----------------------------------------
   l_orderkey, //订单标识
   o_orderdate, //订单日期
   o_shippriority //运输优先级
   order by //排序操作
   revenue desc, //降序排序，把潜在最大收入列在前面
   o_orderdate;


    */
        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"oneOfRevenue",/*outputName=*/"partial_sum_revenue")},
                /*groupByKeys=*/{"l_orderkey","o_orderdate","o_shippriority"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {


        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_sum_revenue",/*outputName=*/"revenue")},
                /*groupByKeys=*/{"l_orderkey","o_orderdate","o_shippriority"});



        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }


};





#endif //OLVP_QUERY3_SHUFFLESTAGE_HPP
