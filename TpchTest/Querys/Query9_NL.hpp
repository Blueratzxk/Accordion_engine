//
// Created by zxk on 5/15/24.
//

#ifndef OLVP_QUERY9_NL_HPP
#define OLVP_QUERY9_NL_HPP





/*
 select
nation,
o_year,
sum(amount) as sum_profit //每个国家每一年所有被定购的零件在一年中的总利润
from
(select
n_name as nation, //国家
extract(year from o_orderdate) as o_year, //取出年份
l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount //利润
from
part,supplier,lineitem,partsupp,orders,nation //六表连接
where
s_suppkey = l_suppkey
and ps_suppkey = l_suppkey
and ps_partkey = l_partkey
and p_partkey = l_partkey
and o_orderkey = l_orderkey
and s_nationkey = n_nationkey
and p_name like '%[COLOR]%' //LIKE操作，查询优化器可能进行优化
) as profit
group by //按国家和年份分组
nation,
o_year
order by //按国家和年份排序，年份大者靠前
nation,
o_year desc;

 * */



#include "../../Query/RegQuery.h"


class Query9_NL:public RegQuery
{

public:
    Query9_NL(){

    }
    string getSql()  {return TpchSqls::Q9();}
    PlanNode* getPlanTree()
    {


        PlanNode *eightTJ = create5TJJoinNation();

        PlanNode *proj = createProjectNode();
        proj->addSource(eightTJ);

        PlanNode *pagg = createPartialAgg();
        pagg->addSource(proj);

        LocalExchangeNode *LocalExchangePagg = new LocalExchangeNode("LocalExchangeJoin");
        LocalExchangePagg->addSource(pagg);


        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,pagg);



        PlanNode *fagg = createFinalAgg();
        fagg->addSource(exchange);

        vector<string> sortKeys = {"nation","o_year"};
        vector<SortOperator::SortOrder> sortOrders = {SortOperator::Ascending,SortOperator::Descending};
        SortDescriptor sortDescriptor(sortKeys,sortOrders);
        SortNode *sort = new SortNode(UUID::create_uuid(),sortDescriptor);
        sort->addSource(fagg);



        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(sort);
        return (PlanNode*)output;
    }




    PartialAggregationNode *createPartialAgg()
    {



        AggregationDesc partialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"amount",/*outputName=*/"partial_amount")},
                /*groupByKeys=*/{"nation","o_year"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),partialAggDesc);

        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_amount",/*outputName=*/"amount")},
                /*groupByKeys=*/{"nation","o_year"});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);

        return finalAggregationNode;

    }



    ProjectNode *createProjectNode()
    {

        //FieldDesc("l_quantity","int64"),
        // FieldDesc("l_extendedprice","double"),
        // FieldDesc("l_discount","double"),
        // FieldDesc("ps_supplycost","double"),
        // FieldDesc("o_orderdate","date32"),
        // FieldDesc("n_name","string")

        DoubleLiteral *const1 = new DoubleLiteral("0","1");
        Column *col_l_discount = new Column("0","l_discount","double");

        FunctionCall *disc_price_sub = new FunctionCall("0","subtract","double");
        disc_price_sub->addChilds({const1,col_l_discount});


        Column *col_l_extendedprice = new Column("0","l_extendedprice","double");

        FunctionCall *disc_price = new FunctionCall("0","multiply","double");
        disc_price->addChilds({disc_price_sub,col_l_extendedprice});



        Column *ps_supplycost = new Column("0","ps_supplycost","double");
        Column *l_quantity = new Column("0","l_quantity","int64");

        FunctionCall *castL_quantityDouble = new FunctionCall("0","castFLOAT8","double");
        castL_quantityDouble->addChilds({l_quantity});


        FunctionCall *costMulQuantity = new FunctionCall("0","multiply","double");
        costMulQuantity->addChilds({ps_supplycost,castL_quantityDouble});

        FunctionCall *sub = new FunctionCall("0","subtract","double");
        sub->addChilds({disc_price,costMulQuantity});



        Column *o_orderdate = new Column("0","o_orderdate","date32");
        FunctionCall *castDate = new FunctionCall("0","castDATE","date64");
        castDate->addChilds({o_orderdate});

        FunctionCall *extractYear = new FunctionCall("0","extractYear","int64");
        extractYear->addChilds({castDate});


        ProjectAssignments assignments;



        //l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        assignments.addAssignment(FieldDesc("l_quantity","int64"),FieldDesc("amount","double"),sub);
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("ps_supplycost","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderdate","date32"), FieldDesc("o_year","int64"),extractYear);
        assignments.addAssignment(FieldDesc("n_name","string"),FieldDesc("nation","string"),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }

    PlanNode *create5TJJoinNation() {


        PlanNode *LJFPJSJPSJO = createLineitemJoinFilteredPartJoinSupplierJoinPartSuppJoinOrders();
        TableScanNode *tableScanNation = createTableScanNation();





        vector<FieldDesc> LJFPJSJPSJOProbeSchema = {FieldDesc("l_quantity","int64"),
                                                    FieldDesc("l_extendedprice","double"),
                                                    FieldDesc("l_discount","double"),
                                                    FieldDesc("s_nationkey","int64"),
                                                    FieldDesc("ps_supplycost","double"),
                                                    FieldDesc("o_orderdate","date32")};


        vector<FieldDesc> nationBuildSchema = {FieldDesc("n_nationkey","int64"),
                                               FieldDesc("n_name","string"),
                                               FieldDesc("n_regionkey","int64"),
                                               FieldDesc("n_comment","string")};




        //FieldDesc("l_quantity","int64"), FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("ps_supplycost","double"),FieldDesc("o_orderdate","date32"),FieldDesc("n_name","string")

        vector<FieldDesc> nationBuildOutputSchema = {FieldDesc("n_name","string")};
        vector<int> LJFPJSJPSJOprobeOutputChannels = {0,1,2,4,5};
        vector<int> LJFPJSJPSJOprobeHashChannels = {3};
        vector<int> nationbuildOutputChannels = {1};
        vector<int> nationbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(LJFPJSJPSJOProbeSchema,LJFPJSJPSJOprobeHashChannels,LJFPJSJPSJOprobeOutputChannels,nationBuildSchema,nationbuildHashChannels,nationbuildOutputChannels,nationBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,LJFPJSJPSJO);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanNation);


        LocalExchangeNode *LocalExchangeProbe = new LocalExchangeNode("LocalExchangeProbe");
        LocalExchangeProbe->addSource(probeExchange);




        Join->addProbe(LJFPJSJPSJO);
        Join->addBuild(buildExchange);


        return Join;

    }

    PlanNode *createLineitemJoinFilteredPartJoinSupplierJoinPartSuppJoinOrders() {


        PlanNode *LJFPJSJPS = createLineitemJoinFilteredPartJoinSupplierJoinPartSupp();
        ProjectNode *tableScanOrders = createTablescanOrders();





        vector<FieldDesc> LJFPJSJPSProbeSchema = {FieldDesc("l_orderkey","int64"),
                                                  FieldDesc("l_quantity","int64"),
                                                  FieldDesc("l_extendedprice","double"),
                                                  FieldDesc("l_discount","double"),
                                                  FieldDesc("s_nationkey","int64"),
                                                  FieldDesc("ps_supplycost","double")};


        vector<FieldDesc> ordersBuildSchema = {FieldDesc("o_orderkey","int64"),
                                               FieldDesc("o_orderdate","date32")};



        //  FieldDesc("l_quantity","int64"),FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("s_nationkey","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("o_orderdate","date32")
        vector<FieldDesc> ordersBuildOutputSchema = {FieldDesc("o_orderdate","date32")};
        vector<int> LJFPJSJPSprobeOutputChannels = {1,2,3,4,5};
        vector<int> LJFPJSJPSprobeHashChannels = {0};
        vector<int> ordersbuildOutputChannels = {1};
        vector<int> ordersbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(LJFPJSJPSProbeSchema,LJFPJSJPSprobeHashChannels,LJFPJSJPSprobeOutputChannels,ordersBuildSchema,ordersbuildHashChannels,ordersbuildOutputChannels,ordersBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,LJFPJSJPS);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanOrders);



        LocalExchangeNode *LocalExchangeProbe = new LocalExchangeNode("LocalExchangeProbe");
        LocalExchangeProbe->addSource(probeExchange);




        Join->addProbe(LJFPJSJPS);
        Join->addBuild(buildExchange);


        LocalExchangeNode *LocalExchangeJoin = new LocalExchangeNode("LocalExchangeJoin");
        LocalExchangeJoin->addSource(Join);




        return Join;
    }

    PlanNode *createLineitemJoinFilteredPartJoinSupplierJoinPartSupp() {

        PlanNode *LJFPJS = createLineitemJoinFilteredPartJoinSupplier();
        ProjectNode *tableScanPartsupp = createTableScanPartSupp();





        vector<FieldDesc> LJFPJSProbeSchema = {FieldDesc("l_orderkey","int64"),
                                               FieldDesc("l_partkey","int64"),
                                               FieldDesc("l_suppkey","int64"),
                                               FieldDesc("l_quantity","int64"),
                                               FieldDesc("l_extendedprice","double"),
                                               FieldDesc("l_discount","double"),
                                               FieldDesc("s_nationkey","int64")};


        vector<FieldDesc> partSuppBuildSchema = {FieldDesc("ps_partkey","int64"),
                                                 FieldDesc("ps_suppkey","int64"),
                                                 FieldDesc("ps_supplycost","double")};



        // FieldDesc("l_orderkey","int64"),FieldDesc("l_quantity","int64"),FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("s_nationkey","int64"),FieldDesc("ps_supplycost","double")

        vector<FieldDesc> partSuppBuildOutputSchema = {FieldDesc("ps_supplycost","double")};
        vector<int> LLJFPJSprobeOutputChannels = {0,3,4,5,6};
        vector<int> LJFPJSprobeHashChannels = {1,2};
        vector<int> partSuppbuildOutputChannels = {2};
        vector<int> partSuppbuildHashChannels = {0,1};
        LookupJoinDescriptor lookupJoinDescriptor(LJFPJSProbeSchema,LJFPJSprobeHashChannels,LLJFPJSprobeOutputChannels,partSuppBuildSchema,partSuppbuildHashChannels,partSuppbuildOutputChannels,partSuppBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,LJFPJS);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanPartsupp);




        LocalExchangeNode *LocalExchangeProbe = new LocalExchangeNode("LocalExchangeProbe");
        LocalExchangeProbe->addSource(probeExchange);




        Join->addProbe(LJFPJS);
        Join->addBuild(buildExchange);


        LocalExchangeNode *LocalExchangeJoin = new LocalExchangeNode("LocalExchangeJoin");
        LocalExchangeJoin->addSource(Join);



        return Join;

    }


    PlanNode *createLineitemJoinFilteredPartJoinSupplier() {

        PlanNode *LJFP = this->createLineitemJoinFilteredPart();
        PlanNode *tableScanSupplier = this->createTableScanSupplier();


        vector<FieldDesc> LJFPProbeSchema = {FieldDesc("l_orderkey","int64"),
                                             FieldDesc("l_partkey","int64"),
                                             FieldDesc("l_suppkey","int64"),
                                             FieldDesc("l_quantity","int64"),
                                             FieldDesc("l_extendedprice","double"),
                                             FieldDesc("l_discount","double"),
                                             FieldDesc("p_partkey", "int64")};


        vector<FieldDesc> supplierBuildSchema = {FieldDesc("s_suppkey","int64"),
                                                 FieldDesc("s_name","string"),
                                                 FieldDesc("s_address","int64"),
                                                 FieldDesc("s_nationkey","int64"),
                                                 FieldDesc("s_phone","string"),
                                                 FieldDesc("s_acctbal","double"),
                                                 FieldDesc("s_comment","string")};


        //FieldDesc("l_orderkey","int64"),FieldDesc("l_partkey","int64"),FieldDesc("l_quantity","int64"),FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("s_nationkey","int64")
        vector<FieldDesc> supplierBuildOutputSchema = {FieldDesc("s_nationkey","int64")};
        vector<int> LJFPprobeOutputChannels = {0,1,2,3,4,5};
        vector<int> LJFPprobeHashChannels = {2};
        vector<int> supplierbuildOutputChannels = {3};
        vector<int> supplierbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(LJFPProbeSchema,LJFPprobeHashChannels,LJFPprobeOutputChannels,supplierBuildSchema,supplierbuildHashChannels,supplierbuildOutputChannels,supplierBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,LJFP);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanSupplier);


        LocalExchangeNode *LocalExchangeProbe = new LocalExchangeNode("LocalExchangeProbe");
        LocalExchangeProbe->addSource(probeExchange);




        Join->addProbe(LJFP);
        Join->addBuild(buildExchange);


        LocalExchangeNode *LocalExchangeJoin = new LocalExchangeNode("LocalExchangeJoin");
        LocalExchangeJoin->addSource(Join);

        return Join;




    }

    PlanNode *createLineitemJoinFilteredPart()
    {


        PlanNode *tableScanLineitem = this->createTableScanLineitem();
        PlanNode *fpart = this->createTableScanPartAndFilter();


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

        //FieldDesc("l_orderkey","int64"),FieldDesc("l_partkey","int64"),FieldDesc("l_suppkey","int64"),FieldDesc("p_partkey", "int64")

        vector<FieldDesc> fpartBuildSchema = {FieldDesc("p_partkey", "int64"),
                                              FieldDesc("p_name", "string"),
                                              FieldDesc("p_mfgr", "string"),
                                              FieldDesc("p_brand", "string"),
                                              FieldDesc("p_type", "string"),
                                              FieldDesc("p_size", "int64"),
                                              FieldDesc("p_container", "string"),
                                              FieldDesc("p_retailprice", "double"),
                                              FieldDesc("p_comment", "string")};

        vector<FieldDesc> fpartBuildOutputSchema = {FieldDesc("p_partkey", "int64")};

        vector<int> lineitemProbeOutputChannels = {0,1,2,4,5,6};
        vector<int> lineitemProbeHashChannels = {1};
        vector<int> fpartbuildOutputChannels = {0};
        vector<int> fpartbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(lineitemProbeSchema,lineitemProbeHashChannels,lineitemProbeOutputChannels,fpartBuildSchema,fpartbuildHashChannels,fpartbuildOutputChannels,fpartBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fpart);



        LocalExchangeNode *LocalExchangeProbe = new LocalExchangeNode("LocalExchangeProbe");
        LocalExchangeProbe->addSource(probeExchange);




        lookupJoinNode->addProbe(probeExchange);
        lookupJoinNode->addBuild(buildExchange);


        LocalExchangeNode *LocalExchangeJoin = new LocalExchangeNode("LocalExchangeJoin");
        LocalExchangeJoin->addSource(lookupJoinNode);


        return lookupJoinNode;


    }


    FilterNode *createOrdersTableScanAndFilter()
    {


        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        Column *o_orderdate = new Column("0","o_orderdate","date32");

        Date32Literal *date32Literalup = new Date32Literal("0","1996-12-31");


        FunctionCall *castUp = new FunctionCall("0","castDATE","date32");
        castUp->addChilds({date32Literalup});


        FunctionCall *o_orderdate_lessthanequal = new FunctionCall("0","less_than_or_equal_to","bool");
        o_orderdate_lessthanequal->addChilds({o_orderdate,castUp});


        Date32Literal *date32Literaldown = new Date32Literal("0","1995-01-01");
        FunctionCall *castDown = new FunctionCall("0","castDATE","date32");
        castDown->addChilds({date32Literaldown});

        FunctionCall *o_orderdate_greaterequal = new FunctionCall("0","greater_than_or_equal_to","bool");
        o_orderdate_greaterequal->addChilds({o_orderdate,castDown});


        FunctionCall *orderdataAnd = new FunctionCall("0","and","bool");
        orderdataAnd->addChilds({o_orderdate_lessthanequal,o_orderdate_greaterequal});





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

    ProjectNode *createTableScanPartSupp()
    {
        TableScanNode *tableScanPartSupp = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","partsupp"));


        ProjectAssignments assignments;


        assignments.addAssignment(FieldDesc("ps_partkey","int64"),FieldDesc("ps_partkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("ps_suppkey","int64"),FieldDesc("ps_suppkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("ps_availqty","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("ps_supplycost","double"),FieldDesc("ps_supplycost","double"),NULL);
        assignments.addAssignment(FieldDesc("ps_comment","string"),FieldDesc::getEmptyDesc(),NULL);

        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);


        projectNode->addSource(tableScanPartSupp);



        return projectNode;

    }

    ProjectNode *createTablescanOrders()
    {
        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        ProjectAssignments assignments;
        assignments.addAssignment(FieldDesc("o_orderkey","int64"),FieldDesc("o_orderkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("o_custkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderstatus","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_totalprice","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderdate","date32"),FieldDesc("o_orderdate","date32"),NULL);
        assignments.addAssignment(FieldDesc("o_orderpriority","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_clerk","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_shippriority","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_comment","string"),FieldDesc::getEmptyDesc(),NULL);
        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);


        projectNode->addSource(tableScanOrders);

        return projectNode;


    }
    TableScanNode *createTableScanLineitem()
    {
        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));
        return tableScanLineitem;

    }

    PlanNode *createTableScanPartAndFilter()
    {

        TableScanNode *tableScanPart = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","part"));


        StringLiteral *p_nameLike = new StringLiteral("0","aquamarine drab dim antique sky");
        Column *p_name = new Column("0","p_name","string");

        FunctionCall *equal  = new FunctionCall("0","like","bool");
        equal->addChilds({p_name,p_nameLike});


        //p_partkey|p_name|p_mfgr|p_brand|p_type|p_size|p_container|p_retailprice|p_comment|
        vector<FieldDesc> fieldsIn = {FieldDesc("p_partkey", "int64"),
                                      FieldDesc("p_name", "string"),
                                      FieldDesc("p_mfgr", "string"),
                                      FieldDesc("p_brand", "string"),
                                      FieldDesc("p_type", "string"),
                                      FieldDesc("p_size", "int64"),
                                      FieldDesc("p_container", "string"),
                                      FieldDesc("p_retailprice", "double"),
                                      FieldDesc("p_comment", "string")};

        FilterDescriptor filterDescriptor(fieldsIn, equal);
        FilterNode *filterPart = new FilterNode(UUID::create_uuid(), filterDescriptor);
        filterPart->addSource(tableScanPart);

        return filterPart;

    }

    TableScanNode *createTableScanNation() {
        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "nation"));
        return tableScanNation;
    }


    TableScanNode *createTableScanSupplier()
    {
        TableScanNode *tableScanSupplier = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "supplier"));

        return tableScanSupplier;

    }
    TableScanNode *createTableScanCustomer() {
        TableScanNode *tableScanCustomer = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "customer"));

        return tableScanCustomer;
    }


};





#endif //OLVP_QUERY9_NL_HPP
