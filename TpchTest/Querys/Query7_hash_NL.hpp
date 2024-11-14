//
// Created by zxk on 5/15/24.
//

#ifndef OLVP_QUERY7_HASH_NL_HPP
#define OLVP_QUERY7_HASH_NL_HPP






#include "../../Query/RegQuery.h"
/*
select
supp_nation, //供货商国家
cust_nation, //顾客国家
l_year, sum(volume) as revenue //年度、年度的货运收入
from ( //子查询
select
n1.n_name as supp_nation,
n2.n_name as cust_nation,
extract(year from l_shipdate) as l_year,
l_extendedprice * (1 - l_discount) as volume
from
supplier,lineitem,orders,customer,nation n1,nation n2 //六表连接
where
s_suppkey = l_suppkey
and o_orderkey = l_orderkey
and c_custkey = o_custkey
and s_nationkey = n1.n_nationkey
and c_nationkey = n2.n_nationkey
and ( // NATION2和NATION1的值不同，表示查询的是跨国的货运情况
(n1.n_name = 'CANADA' and n2.n_name = 'AMERICA')
or (n1.n_name = 'AMERICA' and n2.n_name = 'CANADA')
)
and l_shipdate between date '1995-01-01' and date '1996-12-31'
) as shipping
group by
supp_nation,
cust_nation,
l_year
order by
supp_nation,
cust_nation,
l_year;

*/


class Query7_hash_NL:public RegQuery
{

public:
    Query7_hash_NL(){

    }
    string getSql()  {return TpchSqls::Q7();}
    PlanNode* getPlanTree()
    {


        PlanNode *join = create6TJFilter();

        PlanNode *proj = create6TJFilterProj();
        proj->addSource(join);

        PlanNode *partialAgg = createPartialAgg();
        partialAgg->addSource(proj);

        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(partialAgg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,partialAgg);


        PlanNode *finalAgg = createFinalAgg();
        finalAgg->addSource(exchange);

        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(finalAgg);
        return (PlanNode*)output;
    }



    PartialAggregationNode *createPartialAgg()
    {



        AggregationDesc partialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"volume",/*outputName=*/"partial_revenue")},
                /*groupByKeys=*/{"supp_nation","cust_nation","l_year"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),partialAggDesc);

        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_revenue",/*outputName=*/"revenue")},
                /*groupByKeys=*/{"supp_nation","cust_nation","l_year"});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);

        return finalAggregationNode;

    }



    ProjectNode *create6TJFilterProj()
    {


        /*
         * FieldDesc("l_extendedprice","double"),
           FieldDesc("l_discount","double"),
           FieldDesc("l_shipdate","date32"),
           FieldDesc("n1_n_name","string"),
           FieldDesc("n2_n_name","string")
         *
         * */


        DoubleLiteral *const1 = new DoubleLiteral("0","1");
        Column *col_l_discount = new Column("0","l_discount","double");

        FunctionCall *disc_price_sub = new FunctionCall("0","subtract","double");
        disc_price_sub->addChilds({const1,col_l_discount});


        Column *col_l_extendedprice = new Column("0","l_extendedprice","double");

        FunctionCall *disc_price = new FunctionCall("0","multiply","double");
        disc_price->addChilds({disc_price_sub,col_l_extendedprice});





        Column *l_shipdate = new Column("0","l_shipdate","date32");
        FunctionCall *castDate = new FunctionCall("0","castDATE","date64");
        castDate->addChilds({l_shipdate});

        FunctionCall *extractYear = new FunctionCall("0","extractYear","int64");
        extractYear->addChilds({castDate});




        ProjectAssignments assignments;


        //l_extendedprice*(1-l_discount)
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("volume","double"),disc_price);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_shipdate","date32"),FieldDesc("l_year","int64"),extractYear);
        assignments.addAssignment(FieldDesc("n1_n_name","string"),FieldDesc("supp_nation","string"),NULL);
        assignments.addAssignment(FieldDesc("n2_n_name","string"),FieldDesc("cust_nation","string"),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }

    FilterNode *create6TJFilter()
    {

        Column *n1_n_name = new Column("0", "n1_n_name", "string");
        Column *n2_n_name = new Column("0", "n2_n_name", "string");
        StringLiteral *nNameLiteral1 = new StringLiteral("0", "CANADA");
        StringLiteral *nNameLiteral2 = new StringLiteral("0", "CHINA");
        FunctionCall *nNameOr1 = new FunctionCall("0", "or", "bool");
        FunctionCall *nNameOr2 = new FunctionCall("0", "or", "bool");

        {

            FunctionCall *nNameEqual1 = new FunctionCall("0", "equal", "bool");
            nNameEqual1->addChilds({n1_n_name, nNameLiteral1});
            FunctionCall *nNameEqual2 = new FunctionCall("0", "equal", "bool");
            nNameEqual2->addChilds({n2_n_name, nNameLiteral1});
            nNameOr1->addChilds({nNameEqual1, nNameEqual2});
        }

        {
            FunctionCall *nNameEqual1 = new FunctionCall("0", "equal", "bool");
            nNameEqual1->addChilds({n1_n_name, nNameLiteral2});
            FunctionCall *nNameEqual2 = new FunctionCall("0", "equal", "bool");
            nNameEqual2->addChilds({n2_n_name, nNameLiteral2});
            nNameOr2->addChilds({nNameEqual1, nNameEqual2});
        }

        FunctionCall *nNameAnd = new FunctionCall("0", "and", "bool");
        nNameAnd->addChilds({nNameOr1,nNameOr2});

        PlanNode *join = create6TJ();

        vector<FieldDesc> fieldsIn = { FieldDesc("l_extendedprice","double"),
                                       FieldDesc("l_discount","double"),
                                       FieldDesc("l_shipdate","date32"),
                                       FieldDesc("n1_n_name","string"),
                                       FieldDesc("n2_n_name","string")};



        FilterDescriptor filterDescriptor(fieldsIn,nNameAnd);
        FilterNode *filterNation = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterNation->addSource(join);
        return filterNation;




    }


    LookupJoinNode *create6TJ()
    {

        PlanNode *fNJSJL = createFilteredNationJoinSupplierJoinLineitem();
        PlanNode *fNJCJO = createFilteredNationJoinCustomerJoinOrders();

        vector<FieldDesc> fNJSJLProbeSchema = {FieldDesc("l_orderkey","int64"),
                                               FieldDesc("l_extendedprice","double"),
                                               FieldDesc("l_discount","double"),
                                               FieldDesc("l_shipdate","date32"),
                                               FieldDesc("n1_n_name","string")};



        vector<FieldDesc> fNJCJOBuildSchema = {FieldDesc("o_orderkey","int64"),
                                               FieldDesc("n2_n_name","string")};



        vector<FieldDesc> fNJCJOBuildOutputSchema = {FieldDesc("n2_n_name","string")};
        vector<int> fNJSJLprobeOutputChannels = {1,2,3,4};
        vector<int> fNJSJLprobeHashChannels = {0};
        vector<int> fNJCJObuildOutputChannels = {1};
        vector<int> fNJCJObuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(fNJSJLProbeSchema,fNJSJLprobeHashChannels,fNJSJLprobeOutputChannels,fNJCJOBuildSchema,fNJCJObuildHashChannels,fNJCJObuildOutputChannels,fNJCJOBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(fNJCJO);

        LocalExchangeNode *joinLocalExchange2 = new LocalExchangeNode("joinLocalExchange2");
        joinLocalExchange2->addSource(fNJSJL);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,fNJSJL);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fNJCJO);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        Join->addProbe(probeExchange);
        Join->addBuild(buildExchange);


        return Join;


    }


    LookupJoinNode *createFilteredNationJoinCustomer()
    {

        FilterNode *fNation = createTableScanNation1AndFilter();
        TableScanNode *tableScanCustomer = createTableScanCustomer();


        vector<FieldDesc> customerProbeSchema = {FieldDesc("c_custkey","int64"),
                                                 FieldDesc("c_name","string"),
                                                 FieldDesc("c_address","string"),
                                                 FieldDesc("c_nationkey","int64"),
                                                 FieldDesc("c_phone","string"),
                                                 FieldDesc("c_acctbal","double"),
                                                 FieldDesc("c_mktsegment","string"),
                                                 FieldDesc("c_comment","string")};


        vector<FieldDesc> fnationBuildSchema = {FieldDesc("n_nationkey","int64"),
                                                FieldDesc("n_name","string"),
                                                FieldDesc("n_regionkey","int64"),
                                                FieldDesc("n_comment","string")};


        //FieldDesc("c_custkey","int64"),FieldDesc("n_name","string"),
        vector<FieldDesc> fnationBuildOutputSchema = {FieldDesc("n2_n_name","string")};
        vector<int> customerprobeOutputChannels = {0};
        vector<int> customerprobeHashChannels = {3};
        vector<int> fnationbuildOutputChannels = {1};
        vector<int> fnationbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(customerProbeSchema,customerprobeHashChannels,customerprobeOutputChannels,fnationBuildSchema,fnationbuildHashChannels,fnationbuildOutputChannels,fnationBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"3"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;



        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fNation);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanCustomer);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        Join->addProbe(probeExchange);
        Join->addBuild(buildExchange);


        return Join;


    }

    LookupJoinNode *createFilteredNationJoinCustomerJoinOrders()
    {

        LookupJoinNode *fNJC = createFilteredNationJoinCustomer();
        TableScanNode *tableScanOrders = createTableScanOrders();


        vector<FieldDesc> ordersProbeSchema = {FieldDesc("o_orderkey","int64"),
                                               FieldDesc("o_custkey","int64"),
                                               FieldDesc("o_orderstatus","string"),
                                               FieldDesc("o_totalprice","double"),
                                               FieldDesc("o_orderdate","date32"),
                                               FieldDesc("o_orderpriority","string"),
                                               FieldDesc("o_clerk","string"),
                                               FieldDesc("o_shippriority","int64"),
                                               FieldDesc("o_comment","string")};


        vector<FieldDesc> fNJCBuildSchema = {FieldDesc("c_custkey","int64"),
                                             FieldDesc("n2_n_name","string")};


        //FieldDesc("o_orderkey","int64"),FieldDesc("n2_n_name","string")
        vector<FieldDesc> fNJCBuildOutputSchema = {FieldDesc("n2_n_name","string")};
        vector<int> ordersprobeOutputChannels = {0};
        vector<int> ordersprobeHashChannels = {1};
        vector<int> fNJCbuildOutputChannels = {1};
        vector<int> fNJCbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(ordersProbeSchema,ordersprobeHashChannels,ordersprobeOutputChannels,fNJCBuildSchema,fNJCbuildHashChannels,fNJCbuildOutputChannels,fNJCBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(fNJC);

        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"1"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fNJC);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanOrders);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);


        Join->addProbe(probeExchange);
        Join->addBuild(buildExchange);


        return Join;


    }

    TableScanNode *createTableScanCustomer()
    {
        TableScanNode *tableScanCustomer = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "customer"));

        return tableScanCustomer;

    }

    TableScanNode *createTableScanSupplier()
    {
        TableScanNode *tableScanSupplier = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "supplier"));

        return tableScanSupplier;

    }

    FilterNode *createTableScanLineitemAndFilter()
    {

        Column *l_shipdate = new Column("0","l_shipdate","date32");

        Date32Literal *dateUp = new Date32Literal("0","1996-12-31");
        Date32Literal *dateDown = new Date32Literal("0","1995-01-01");

        FunctionCall *castDateLiteralUp = new FunctionCall("0","castDATE","date32");
        castDateLiteralUp->addChilds({dateUp});
        FunctionCall *castDateLiteralDown = new FunctionCall("0","castDATE","date32");
        castDateLiteralDown->addChilds({dateDown});


        FunctionCall *l_shipdate_greaterEqual =  new FunctionCall("0","greater_than_or_equal_to","bool");
        l_shipdate_greaterEqual->addChilds({l_shipdate,castDateLiteralDown});
        FunctionCall *l_shipdate_lessEqual =  new FunctionCall("0","less_than_or_equal_to","bool");
        l_shipdate_lessEqual->addChilds({l_shipdate,castDateLiteralUp});

        FunctionCall *l_discount_and =  new FunctionCall("0","and","bool");
        l_discount_and->addChilds({l_shipdate_greaterEqual,l_shipdate_lessEqual});


        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "lineitem"));




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



        FilterDescriptor filterDescriptor(fieldsIn,l_discount_and);
        FilterNode *filterLineitem = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterLineitem->addSource(tableScanLineitem);



        return filterLineitem;

    }


    TableScanNode *createTableScanOrders()
    {
        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "orders"));

        return tableScanOrders;

    }

    FilterNode *createTableScanNation1AndFilter() {


        Column *n_name = new Column("0", "n_name", "string");
        StringLiteral *nNameLiteral1 = new StringLiteral("0", "CANADA");

        FunctionCall *nNameEqual1 = new FunctionCall("0", "equal", "bool");
        nNameEqual1->addChilds({n_name, nNameLiteral1});


        StringLiteral *nNameLiteral2 = new StringLiteral("0", "CHINA");

        FunctionCall *nNameEqual2 = new FunctionCall("0", "equal", "bool");
        nNameEqual2->addChilds({n_name, nNameLiteral2});


        FunctionCall *nNameOr = new FunctionCall("0","or","bool");
        nNameOr->addChilds({nNameEqual1,nNameEqual2});



        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "nation"));




        vector<FieldDesc> fieldsIn = { FieldDesc("n_nationkey","int64"),
                                       FieldDesc("n_name","string"),
                                       FieldDesc("n_regionkey","int64"),
                                       FieldDesc("n_comment","string")};



        FilterDescriptor filterDescriptor(fieldsIn,nNameOr);
        FilterNode *filterNation = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterNation->addSource(tableScanNation);
        return filterNation;


    }

    LookupJoinNode *createFilteredNationJoinSupplierJoinLineitem() {

        LookupJoinNode *fNJS = createFilteredNationJoinSupplier();
        FilterNode *tableScanLineitem = createTableScanLineitemAndFilter();


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


        vector<FieldDesc> fNJSBuildSchema = {FieldDesc("s_suppkey","int64"),
                                             FieldDesc("n1_n_name","string")};


        //FieldDesc("l_orderkey","int64"),FieldDesc("n1_n_name","string")

        vector<FieldDesc> fNJSBuildOutputSchema = {FieldDesc("n1_n_name","string")};
        vector<int> lineitemprobeOutputChannels = {0,5,6,10};
        vector<int> lineitemprobeHashChannels = {2};
        vector<int> fNJSbuildOutputChannels = {1};
        vector<int> fNJSbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(lineitemProbeSchema,lineitemprobeHashChannels,lineitemprobeOutputChannels,fNJSBuildSchema,fNJSbuildHashChannels,fNJSbuildOutputChannels,fNJSBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(fNJS);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"2"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fNJS);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        Join->addProbe(probeExchange);
        Join->addBuild(buildExchange);


        return Join;

    }

    LookupJoinNode *createFilteredNationJoinSupplier()
    {

        FilterNode *fNation = createTableScanNation2AndFilter();
        TableScanNode *tableScanSupplier = createTableScanSupplier();


        vector<FieldDesc> supplierProbeSchema = {FieldDesc("s_suppkey","int64"),
                                                 FieldDesc("s_name","string"),
                                                 FieldDesc("s_address","int64"),
                                                 FieldDesc("s_nationkey","int64"),
                                                 FieldDesc("s_phone","string"),
                                                 FieldDesc("s_acctbal","double"),
                                                 FieldDesc("s_comment","string")};


        vector<FieldDesc> fnationBuildSchema = {FieldDesc("n_nationkey","int64"),
                                                FieldDesc("n_name","string"),
                                                FieldDesc("n_regionkey","int64"),
                                                FieldDesc("n_comment","string")};


        //FieldDesc("s_suppkey","int64"),FieldDesc("n_name","string"),
        vector<FieldDesc> fnationBuildOutputSchema = {FieldDesc("n1_n_name","string")};
        vector<int> supplierprobeOutputChannels = {0};
        vector<int> supplierprobeHashChannels = {3};
        vector<int> fnationbuildOutputChannels = {1};
        vector<int> fnationbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(supplierProbeSchema,supplierprobeHashChannels,supplierprobeOutputChannels,fnationBuildSchema,fnationbuildHashChannels,fnationbuildOutputChannels,fnationBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"3"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanSupplier);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fNation);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);



        Join->addProbe(probeExchange);
        Join->addBuild(buildExchange);


        return Join;


    }

    FilterNode *createTableScanNation2AndFilter() {


        Column *n_name = new Column("0", "n_name", "string");
        StringLiteral *nNameLiteral1 = new StringLiteral("0", "CANADA");

        FunctionCall *nNameEqual1 = new FunctionCall("0", "equal", "bool");
        nNameEqual1->addChilds({n_name, nNameLiteral1});


        StringLiteral *nNameLiteral2 = new StringLiteral("0", "CHINA");

        FunctionCall *nNameEqual2 = new FunctionCall("0", "equal", "bool");
        nNameEqual2->addChilds({n_name, nNameLiteral2});


        FunctionCall *nNameOr = new FunctionCall("0","or","bool");
        nNameOr->addChilds({nNameEqual1,nNameEqual2});



        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "nation"));




        vector<FieldDesc> fieldsIn = { FieldDesc("n_nationkey","int64"),
                                       FieldDesc("n_name","string"),
                                       FieldDesc("n_regionkey","int64"),
                                       FieldDesc("n_comment","string")};



        FilterDescriptor filterDescriptor(fieldsIn,nNameOr);
        FilterNode *filterNation = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterNation->addSource(tableScanNation);
        return filterNation;

    }





};



#endif //OLVP_QUERY7_HASH_NL_HPP
