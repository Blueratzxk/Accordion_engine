//
// Created by zxk on 9/22/23.
//

#ifndef OLVP_QUERY5_HASH_HPP
#define OLVP_QUERY5_HASH_HPP



#include "../../Query/RegQuery.h"


/*

select
n_name,
sum(l_extendedprice * (1 - l_discount)) as revenue //聚集操作
from
customer,orders,lineitem,supplier,nation,region //六表连接
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and l_suppkey = s_suppkey
and c_nationkey = s_nationkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = '[REGION]' //指定地区，在TPC-H标准指定的范围内随机选择
and o_orderdate >= date '[DATE]' //DATE是从1993年到1997年中随机选择的一年的1月1日
and o_orderdate < date '[DATE]' + interval '1' year
group by //按名字分组
n_name
order by //按收入降序排序，注意分组和排序子句不同
revenue desc;

*/





class Query5_hash:public RegQuery
{

public:
    Query5_hash(){

    }
    string getSql()  {return TpchSqls::Q5();}
    PlanNode* getPlanTree()
    {


        PlanNode *join = create4TJSupplier();
        PlanNode *joinTest = test();
        PlanNode *proj = createProject();
        proj->addSource({join});

        PlanNode *patialAgg = createPartialAgg();
        patialAgg->addSource(proj);

        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(patialAgg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,paggLocalExchange);



        PlanNode *finalAgg = createFinalAgg();
        finalAgg->addSource(exchange);



        vector<string> sortKeys = {"revenue"};
        vector<SortOperator::SortOrder> sortOrders = {SortOperator::Descending};
        SortDescriptor sortDescriptor(sortKeys,sortOrders);
        SortNode *sort = new SortNode(UUID::create_uuid(),sortDescriptor);
        sort->addSource(finalAgg);


        shared_ptr<PartitioningScheme> schemeTest = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchangeTest = new ExchangeNode("exchange",ExchangeNode::GATHER,schemeTest,joinTest);


        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(sort);
        return (PlanNode*)output;
    }

    PlanNode *test() {
        PlanNode *RJNJC = this->createRegionJoinNation();
        return RJNJC;
    }

    FilterNode *createTableScanRegionAndFilter() {

        // region build,nation probe

        TableScanNode *tableScanRegion = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "region"));


        Column *r_name = new Column("0", "r_name", "string");
        StringLiteral *rNameLiteral = new StringLiteral("0", "AMERICA");
        FunctionCall *rnameEqual = new FunctionCall("0", "equal", "bool");
        rnameEqual->addChilds({r_name, rNameLiteral});

        vector<FieldDesc> fieldsIn = {FieldDesc("r_regionkey", "int64"),
                                      FieldDesc("r_name", "string"),
                                      FieldDesc("r_comment", "string")};

        FilterDescriptor filterDescriptor(fieldsIn, rnameEqual);
        FilterNode *filterRegion = new FilterNode(UUID::create_uuid(), filterDescriptor);
        filterRegion->addSource(tableScanRegion);

        return filterRegion;

    }

    TableScanNode *createTableScanNation() {
        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "nation"));
        return tableScanNation;
    }

    TableScanNode *createTableScanCustomer() {
        TableScanNode *tableScanCustomer = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "customer"));

        return tableScanCustomer;
    }

    FilterNode *createOrdersTableScanAndFilter()
    {


        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        Column *o_orderdate = new Column("0","o_orderdate","date32");

        Date32Literal *date32Literalup = new Date32Literal("0","1995-09-21");
        FunctionCall *castOrderDate = new FunctionCall("0","castDATE","date32");
        castOrderDate->addChilds({o_orderdate});
        FunctionCall *castOrderDateToDate64 = new FunctionCall("0","castDATE","date64");
        castOrderDateToDate64->addChilds({o_orderdate});


        FunctionCall *castUp = new FunctionCall("0","castDATE","date32");
        castUp->addChilds({date32Literalup});
        FunctionCall *castDate32LiteralupToDate64 = new FunctionCall("0","castDATE","date64");
        castDate32LiteralupToDate64->addChilds({castUp});

        Int32Literal *addOne = new Int32Literal("0","1");
        FunctionCall *addYear = new FunctionCall("0","timestampaddYear","date64");
        addYear->addChilds({castDate32LiteralupToDate64,addOne});





        FunctionCall *o_orderdate_lessthan = new FunctionCall("0","less_than","bool");
        o_orderdate_lessthan->addChilds({castOrderDateToDate64,addYear});




        Date32Literal *date32Literaldown = new Date32Literal("0","1995-03-21");
        FunctionCall *castOrderDate2 = new FunctionCall("0","castDATE","date32");
        castOrderDate2->addChilds({date32Literaldown});

        FunctionCall *o_orderdate_greaterequal = new FunctionCall("0","greater_than_or_equal_to","bool");
        o_orderdate_greaterequal->addChilds({o_orderdate,castOrderDate2});

        FunctionCall *orderdataAnd = new FunctionCall("0","and","bool");
        orderdataAnd->addChilds({o_orderdate_lessthan,o_orderdate_greaterequal});





        vector<FieldDesc> fieldsIn = {FieldDesc("o_orderkey","int64"),
                                      FieldDesc("o_custkey","int64"),
                                      FieldDesc("o_orderstatus","string"),
                                      FieldDesc("o_totalprice","double"),
                                      FieldDesc("o_orderdate","date32"),
                                      FieldDesc("o_orderpriority","string"),
                                      FieldDesc("o_clerk","string"),
                                      FieldDesc("o_shippriority","int64"),
                                      FieldDesc("o_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,orderdataAnd);
        FilterNode *filterOrders = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterOrders->addSource(tableScanOrders);
        return filterOrders;

    }

    TableScanNode *createTableScanSupplier()
    {
        TableScanNode *tableScanSupplier = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));
        return tableScanSupplier;
    }



    LookupJoinNode *createRegionJoinNation()
    {


        PlanNode *filteredRegion = this->createTableScanRegionAndFilter();
        PlanNode *tableScanNation = this->createTableScanNation();


        vector<FieldDesc> nationProbeSchema = {FieldDesc("n_nationkey","int64"),
                                               FieldDesc("n_name","string"),
                                               FieldDesc("n_regionkey","int64"),
                                               FieldDesc("n_comment","string")};


        vector<FieldDesc> fregionBuildSchema = {FieldDesc("r_regionkey","int64"),FieldDesc("r_name","string"),FieldDesc("r_comment","string")};
        vector<FieldDesc> fregionBuildOutputSchema = {FieldDesc("r_regionkey","int64")};
        vector<int> nationprobeOutputChannels = {0,1};
        vector<int> nationprobeHashChannels = {2};
        vector<int> fregionbuildOutputChannels = {0};
        vector<int> fregionbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(nationProbeSchema,nationprobeHashChannels,nationprobeOutputChannels,fregionBuildSchema,fregionbuildHashChannels,fregionbuildOutputChannels,fregionBuildOutputSchema);
        LookupJoinNode *RNJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"2"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,filteredRegion);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanNation);

        LocalExchangeNode *taleScanLocalExchange = new LocalExchangeNode("taleScanLocalExchange");
        taleScanLocalExchange->addSource(probeExchange);

        RNJoin->addProbe(taleScanLocalExchange);
        RNJoin->addBuild(buildExchange);


        return RNJoin;


    }


    LookupJoinNode *createRegionJoinNationJoinCustomer()
    {


        PlanNode *RJN = this->createRegionJoinNation();
        PlanNode *tableScanCustomer = this->createTableScanCustomer();


        vector<FieldDesc> customerProbeSchema = {FieldDesc("c_custkey","int64"),
                                                 FieldDesc("c_name","string"),
                                                 FieldDesc("c_address","string"),
                                                 FieldDesc("c_nationkey","int64"),
                                                 FieldDesc("c_phone","string"),
                                                 FieldDesc("c_acctbal","double"),
                                                 FieldDesc("c_mktsegment","string"),
                                                 FieldDesc("c_comment","string")};

        //FieldDesc("c_custkey","int64"),FieldDesc("n_nationkey","int64"),FieldDesc("n_name","string"),

        vector<FieldDesc> RJNBuildSchema = {FieldDesc("n_nationkey","int64"),
                                            FieldDesc("n_name","string"),
                                            FieldDesc("r_regionkey","int64")};

        vector<FieldDesc> RJNBuildOutputSchema = {FieldDesc("n_nationkey","int64"),
                                                  FieldDesc("n_name","string")};

        vector<int> customerProbeOutputChannels = {0};
        vector<int> customerProbeHashChannels = {3};
        vector<int> RJNbuildOutputChannels = {0,1};
        vector<int> RJNbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(customerProbeSchema,customerProbeHashChannels,customerProbeOutputChannels,RJNBuildSchema,RJNbuildHashChannels,RJNbuildOutputChannels,RJNBuildOutputSchema);
        LookupJoinNode *RJNJCJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        LocalExchangeNode *JoinLocalExchange = new LocalExchangeNode("JoinLocalExchange");
        JoinLocalExchange->addSource(RJN);



        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"3"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanCustomer);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,JoinLocalExchange);

        LocalExchangeNode *tableScanLocalExchange = new LocalExchangeNode("tableScanLocalExchange");
        tableScanLocalExchange->addSource(probeExchange);

        RJNJCJoin->addProbe(tableScanLocalExchange);
        RJNJCJoin->addBuild(buildExchange);


        return RJNJCJoin;


    }

    LookupJoinNode *createRegionJoinNationJoinCustomerJoinOrders()
    {
        PlanNode *RJNJC = this->createRegionJoinNationJoinCustomer();
        FilterNode *filteredOrders = createOrdersTableScanAndFilter();


        vector<FieldDesc> fOrdersProbeSchema = {FieldDesc("o_orderkey","int64"),
                                                FieldDesc("o_custkey","int64"),
                                                FieldDesc("o_orderstatus","string"),
                                                FieldDesc("o_totalprice","double"),
                                                FieldDesc("o_orderdate","date32"),
                                                FieldDesc("o_orderpriority","string"),
                                                FieldDesc("o_clerk","string"),
                                                FieldDesc("o_shippriority","int64"),
                                                FieldDesc("o_comment","string")};

        //FieldDesc("o_orderkey","int64"),FieldDesc("n_nationkey","int64"),FieldDesc("n_name","string")

        vector<FieldDesc> RJNJCBuildSchema = {FieldDesc("c_custkey","int64"),
                                              FieldDesc("n_nationkey","int64"),
                                              FieldDesc("n_name","string")};

        vector<FieldDesc> RJNJCBuildOutputSchema = { FieldDesc("n_nationkey","int64"),
                                                     FieldDesc("n_name","string")};
        vector<int> fOrdersProbeOutputChannels = {0};
        vector<int> fOrdersProbeHashChannels = {1};
        vector<int> RJNJCbuildOutputChannels = {1,2};
        vector<int> RJNJCbuildHashChannels = {0};
        LookupJoinDescriptor LookupJoinDescriptor(fOrdersProbeSchema,fOrdersProbeHashChannels,fOrdersProbeOutputChannels,RJNJCBuildSchema,RJNJCbuildHashChannels,RJNJCbuildOutputChannels,RJNJCBuildOutputSchema);
        LookupJoinNode *RJNJCJOJoin = new LookupJoinNode(UUID::create_uuid(),LookupJoinDescriptor);


        LocalExchangeNode *JoinLocalExchange = new LocalExchangeNode("JoinLocalExchange");
        JoinLocalExchange->addSource(RJNJC);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"1"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,filteredOrders);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,JoinLocalExchange);


        LocalExchangeNode *tableScanLocalExchange = new LocalExchangeNode("tableScanLocalExchange");
        tableScanLocalExchange->addSource(probeExchange);

        RJNJCJOJoin->addProbe(tableScanLocalExchange);
        RJNJCJOJoin->addBuild(buildExchange);

        return RJNJCJOJoin;

    }
    TableScanNode *createTableScanLineitem()
    {
        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));
        return tableScanLineitem;

    }


    LookupJoinNode *createRegionJoinNationJoinCustomerJoinOrdersJoinLineitem(){

        PlanNode *RJNJCJO = this->createRegionJoinNationJoinCustomerJoinOrders();
        PlanNode *tableScanLineitem = createTableScanLineitem();


        vector<FieldDesc> lineitemProbeSchema = {FieldDesc("l_orderkey","int64"),
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

        //FieldDesc("l_suppkey","int64"), FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("n_nationkey","int64"),FieldDesc("n_name","string")

        vector<FieldDesc> RJNJCJOBuildSchema = {FieldDesc("o_orderkey","int64"),
                                                FieldDesc("n_nationkey","int64"),
                                                FieldDesc("n_name","string")};

        vector<FieldDesc> RJNJCJOBuildOutputSchema = { FieldDesc("n_nationkey","int64"),
                                                       FieldDesc("n_name","string")};
        vector<int> lineitemProbeOutputChannels = {2,5,6};
        vector<int> lineitemProbeHashChannels = {0};
        vector<int> RJNJCJObuildOutputChannels = {1,2};
        vector<int> RJNJCJObuildHashChannels = {0};
        LookupJoinDescriptor LookupJoinDescriptor(lineitemProbeSchema,lineitemProbeHashChannels,lineitemProbeOutputChannels,RJNJCJOBuildSchema,RJNJCJObuildHashChannels,RJNJCJObuildOutputChannels,RJNJCJOBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),LookupJoinDescriptor);

        LocalExchangeNode *JoinLocalExchange = new LocalExchangeNode("JoinLocalExchange");
        JoinLocalExchange->addSource(RJNJCJO);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,JoinLocalExchange);

        LocalExchangeNode *tableScanLocalExchange = new LocalExchangeNode("tableScanLocalExchange");
        tableScanLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(tableScanLocalExchange);
        lookupJoinNode->addBuild(buildExchange);

        return lookupJoinNode;


    }



    LookupJoinNode *create4TJSupplier(){


        PlanNode *RJNJCJOJL = this->createRegionJoinNationJoinCustomerJoinOrdersJoinLineitem();
        PlanNode *tableScanSupplier = createTableScanSupplier();


        vector<FieldDesc> RJNJCJOJLProbeSchema = {FieldDesc("l_suppkey","int64"),
                                                  FieldDesc("l_extendedprice","double"),
                                                  FieldDesc("l_discount","double"),
                                                  FieldDesc("n_nationkey","int64"),
                                                  FieldDesc("n_name","string")};



        vector<FieldDesc> supplierBuildSchema ={FieldDesc("s_suppkey","int64"),
                                                FieldDesc("s_name","string"),
                                                FieldDesc("s_address","int64"),
                                                FieldDesc("s_nationkey","int64"),
                                                FieldDesc("s_phone","string"),
                                                FieldDesc("s_acctbal","double"),
                                                FieldDesc("s_comment","string")};


        vector<FieldDesc> supplierBuildOutputSchema = { FieldDesc("s_suppkey","int64")};
        vector<int> RJNJCJOJLProbeOutputChannels = {1,2,4};
        vector<int> RJNJCJOJLProbeHashChannels = {0,3};
        vector<int> supplierbuildOutputChannels = {0};
        vector<int> supplierbuildHashChannels = {0,3};
        LookupJoinDescriptor LookupJoinDescriptor(RJNJCJOJLProbeSchema,RJNJCJOJLProbeHashChannels,RJNJCJOJLProbeOutputChannels,supplierBuildSchema,supplierbuildHashChannels,supplierbuildOutputChannels,supplierBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),LookupJoinDescriptor);

        LocalExchangeNode *JoinLocalExchange = new LocalExchangeNode("JoinLocalExchange");
        JoinLocalExchange->addSource(RJNJCJOJL);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0","3"};
        vector<string> hashColumnBuild = {"0","3"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,JoinLocalExchange);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanSupplier);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);


        lookupJoinNode->addProbe(probeLocalExchange);
        lookupJoinNode->addBuild(buildExchange);

        return lookupJoinNode;

    }


    ProjectNode * createProject()
    {


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
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("oneOfRevenue","double"),disc_price);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("n_name","string"),FieldDesc("n_name","string"),NULL);
        assignments.addAssignment(FieldDesc("s_suppkey","int64"),FieldDesc::getEmptyDesc(),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }


    PartialAggregationNode *createPartialAgg()
    {
        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"oneOfRevenue",/*outputName=*/"partial_revenue")},
                /*groupByKeys=*/{"n_name"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {
        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_revenue",/*outputName=*/"revenue")},
                /*groupByKeys=*/{"n_name"});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }





};


#endif //OLVP_QUERY5_HASH_HPP
