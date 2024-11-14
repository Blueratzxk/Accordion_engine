//
// Created by zxk on 9/22/23.
//

#ifndef OLVP_QUERY8_HASH_HPP
#define OLVP_QUERY8_HASH_HPP


/*
 select
o_year, //年份
sum(case
when nation = '[NATION]'//指定国家，在TPC-H标准指定的范围内随机选择
then volume
else 0
end) / sum(volume) as mkt_share //市场份额：特定种类的产品收入的百分比；聚集操作
from //子查询
(select
extract(year from o_orderdate) as o_year, //分解出年份
l_extendedprice * (1-l_discount) as volume, //特定种类的产品收入
n2.n_name as nation
from
part,supplier,lineitem,orders,customer,nation n1,nation n2,region //八表连接
where
p_partkey = l_partkey
and s_suppkey = l_suppkey
and l_orderkey = o_orderkey
and o_custkey = c_custkey
and c_nationkey = n1.n_nationkey
and n1.n_regionkey = r_regionkey
and r_name = '[REGION]' //指定地区，在TPC-H标准指定的范围内随机选择
and s_nationkey = n2.n_nationkey
and o_orderdate between date '1995-01-01' and date '1996-12-31' //只查95、96年的情况
and p_type = '[TYPE]' //指定零件类型，在TPC-H标准指定的范围内随机选择
) as all_nations
group by //按年分组
o_year
order by //按年排序
o_year;

 * */


#include "../../Query/RegQuery.h"

class Query8_hash:public RegQuery
{

public:
    Query8_hash(){

    }
    string getSql()  {return TpchSqls::Q8();}
    PlanNode* getPlanTree()
    {


        PlanNode *eightTJ = createEightTJ();

        PlanNode *proj = createProjectNode();
        proj->addSource(eightTJ);
        PlanNode *cproj = createConditionVolumeProjectNode();
        cproj->addSource(proj);

        PlanNode *patialAgg = createPartialAgg();
        patialAgg->addSource(cproj);


        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(patialAgg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,paggLocalExchange);


        PlanNode *finalAgg = createFinalAgg();
        finalAgg->addSource(exchange);

        PlanNode *finalProj = createFinalProjectNode();
        finalProj->addSource(finalAgg);

        vector<string> sortKeys = {"o_year"};
        vector<SortOperator::SortOrder> sortOrders = {SortOperator::Ascending};
        SortDescriptor sortDescriptor(sortKeys,sortOrders);
        SortNode *sort = new SortNode(UUID::create_uuid(),sortDescriptor);
        sort->addSource(finalProj);



        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(sort);
        return (PlanNode*)output;
    }




    PartialAggregationNode *createPartialAgg()
    {



        AggregationDesc partialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"volume",/*outputName=*/"partial_volume"),
                                        AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"condition_volume",/*outputName=*/"partial_condition_volume")},
                /*groupByKeys=*/{"o_year"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),partialAggDesc);

        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_volume",/*outputName=*/"volume"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_condition_volume",/*outputName=*/"condition_volume")},
                /*groupByKeys=*/{"o_year"});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);

        return finalAggregationNode;

    }

    ProjectNode *createFinalProjectNode()
    {


        Column *condition_volume = new Column("0","condition_volume","double");
        Column *volume = new Column("0","volume","double");

        FunctionCall *mkt_share = new FunctionCall("0","divide","double");
        mkt_share->addChilds({condition_volume,volume});

        ProjectAssignments assignments;


        //l_extendedprice*(1-l_discount)
        //l_extendedprice|l_discount|o_orderdate|n2_n_name|
        assignments.addAssignment(FieldDesc("o_year","int64"),FieldDesc("o_year","int64"),NULL);
        assignments.addAssignment(FieldDesc("volume","double"),FieldDesc("mkt_share","double"),mkt_share);
        assignments.addAssignment(FieldDesc("condition_volume","double"),FieldDesc::getEmptyDesc(),NULL);





        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;




    }
    ProjectNode *createConditionVolumeProjectNode()
    {


        IfExpression *condition_volumn = new IfExpression("0","double");

        Column *nation = new Column("0","nation","string");
        StringLiteral *nationString = new StringLiteral("0","CHINA");
        FunctionCall *nationEqual = new FunctionCall("0","equal","bool");
        nationEqual->addChilds({nation,nationString});

        Column *volume = new Column("0","volume","double");
        DoubleLiteral *zero = new DoubleLiteral("0","0");
        condition_volumn->setIfBody(nationEqual,volume,zero);

        ProjectAssignments assignments;
        assignments.addAssignment(FieldDesc("volume","double"),FieldDesc("condition_volume","double"),condition_volumn);
        assignments.addAssignment(FieldDesc("volume","double"),FieldDesc("volume","double"),NULL);
        assignments.addAssignment(FieldDesc("o_year","int64"),FieldDesc("o_year","int64"),NULL);
        assignments.addAssignment(FieldDesc("nation","string"),FieldDesc::getEmptyDesc(),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;
    }

    ProjectNode *createProjectNode()
    {


        DoubleLiteral *const1 = new DoubleLiteral("0","1");
        Column *col_l_discount = new Column("0","l_discount","double");

        FunctionCall *disc_price_sub = new FunctionCall("0","subtract","double");
        disc_price_sub->addChilds({const1,col_l_discount});


        Column *col_l_extendedprice = new Column("0","l_extendedprice","double");

        FunctionCall *disc_price = new FunctionCall("0","multiply","double");
        disc_price->addChilds({disc_price_sub,col_l_extendedprice});





        Column *o_orderdate = new Column("0","o_orderdate","date32");
        FunctionCall *castDate = new FunctionCall("0","castDATE","date64");
        castDate->addChilds({o_orderdate});

        FunctionCall *extractYear = new FunctionCall("0","extractYear","int64");
        extractYear->addChilds({castDate});


        ProjectAssignments assignments;


        //l_extendedprice*(1-l_discount)
        //l_extendedprice|l_discount|o_orderdate|n2_n_name|
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("volume","double"),disc_price);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderdate","date32"),FieldDesc("o_year","int64"),extractYear);
        assignments.addAssignment(FieldDesc("n2_n_name","string"),FieldDesc("nation","string"),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;




    }

    LookupJoinNode *createEightTJ()
    {
        LookupJoinNode *sixTJ = this->createFiveTJJoinPart();
        LookupJoinNode *twoTJ = this->createNationJoinSupplier();


        vector<FieldDesc> sixTJProbeSchema = {FieldDesc("l_suppkey","int64"),
                                              FieldDesc("l_extendedprice","double"),
                                              FieldDesc("l_discount","double"),
                                              FieldDesc("o_orderdate","date32"),
                                              FieldDesc("p_partkey", "int64")};


        vector<FieldDesc> twoTJBuildSchema = {FieldDesc("s_suppkey","int64"),
                                              FieldDesc("n2_n_name","string")};



        vector<FieldDesc> twoTJBuildOutputSchema = {FieldDesc("n2_n_name","string")};
        vector<int> sixTJprobeOutputChannels = {1,2,3};
        vector<int> sixTJprobeHashChannels = {0};
        vector<int> twoTJbuildOutputChannels = {1};
        vector<int> twoTJbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(sixTJProbeSchema,sixTJprobeHashChannels,sixTJprobeOutputChannels,twoTJBuildSchema,twoTJbuildHashChannels,twoTJbuildOutputChannels,twoTJBuildOutputSchema);
        LookupJoinNode *Join = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(twoTJ);

        LocalExchangeNode *joinLocalExchange2 = new LocalExchangeNode("joinLocalExchange2");
        joinLocalExchange2->addSource(sixTJ);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,joinLocalExchange2);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,joinLocalExchange);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        Join->addProbe(probeLocalExchange);
        Join->addBuild(buildExchange);


        return Join;



    }


    LookupJoinNode *createNationJoinSupplier()
    {

        TableScanNode *tablescanNation = createTableScanNation2();
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


        //FieldDesc("s_suppkey","int64"),FieldDesc("n2_n_name","string"),
        vector<FieldDesc> fnationBuildOutputSchema = {FieldDesc("n2_n_name","string")};
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
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tablescanNation);


        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);


        Join->addProbe(probeLocalExchange);
        Join->addBuild(buildExchange);


        return Join;


    }

    LookupJoinNode *createFiveTJJoinPart() {


        PlanNode *fiveTJ = this->createRegionJoinNationJoinCustomerJoinOrdersJoinLineitem();
        PlanNode *fpart = this->createTableScanPartAndFilter();

        vector<FieldDesc> fiveTJProbeSchema = {FieldDesc("l_partkey","int64"),
                                               FieldDesc("l_suppkey","int64"),
                                               FieldDesc("l_extendedprice","double"),
                                               FieldDesc("l_discount","double"),
                                               FieldDesc("o_orderdate","date32")};

        //FieldDesc("l_suppkey","int64"),FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("o_orderdate","date32"),FieldDesc("p_partkey", "int64")
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
        vector<int> fiveTJProbeOutputChannels = {1,2,3,4};
        vector<int> fiveTJProbeHashChannels = {0};
        vector<int> fpartbuildOutputChannels = {0};
        vector<int> fpartbuildHashChannels = {0};
        LookupJoinDescriptor LookupJoinDescriptor(fiveTJProbeSchema,fiveTJProbeHashChannels,fiveTJProbeOutputChannels,fpartBuildSchema,fpartbuildHashChannels,fpartbuildOutputChannels,fpartBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),LookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(fiveTJ);

        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,joinLocalExchange);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fpart);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(probeLocalExchange);
        lookupJoinNode->addBuild(buildExchange);

        return lookupJoinNode;




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
        //FieldDesc("l_partkey","int64"), FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("o_orderdate","date32")
        vector<FieldDesc> RJNJCJOBuildSchema = {FieldDesc("o_orderkey","int64"),
                                                FieldDesc("o_orderdate","date32"),
                                                FieldDesc("c_custkey","int64")};

        vector<FieldDesc> RJNJCJOBuildOutputSchema = {FieldDesc("o_orderdate","date32")};
        vector<int> lineitemProbeOutputChannels = {1,2,5,6};
        vector<int> lineitemProbeHashChannels = {0};
        vector<int> RJNJCJObuildOutputChannels = {1};
        vector<int> RJNJCJObuildHashChannels = {0};
        LookupJoinDescriptor LookupJoinDescriptor(lineitemProbeSchema,lineitemProbeHashChannels,lineitemProbeOutputChannels,RJNJCJOBuildSchema,RJNJCJObuildHashChannels,RJNJCJObuildOutputChannels,RJNJCJOBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),LookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(RJNJCJO);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"0"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanLineitem);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,joinLocalExchange);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(probeLocalExchange);
        lookupJoinNode->addBuild(buildExchange);

        return lookupJoinNode;


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

        vector<FieldDesc> RJNJCBuildSchema = {FieldDesc("n_nationkey","int64"),FieldDesc("c_custkey","int64")};

        vector<FieldDesc> RJNJCBuildOutputSchema = {FieldDesc("c_custkey","int64")};
        vector<int> fOrdersProbeOutputChannels = {0,4};
        vector<int> fOrdersProbeHashChannels = {1};
        vector<int> RJNJCbuildOutputChannels = {1};
        vector<int> RJNJCbuildHashChannels = {0};
        LookupJoinDescriptor LookupJoinDescriptor(fOrdersProbeSchema,fOrdersProbeHashChannels,fOrdersProbeOutputChannels,RJNJCBuildSchema,RJNJCbuildHashChannels,RJNJCbuildOutputChannels,RJNJCBuildOutputSchema);
        LookupJoinNode *RJNJCJOJoin = new LookupJoinNode(UUID::create_uuid(),LookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(RJNJC);

        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"1"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,filteredOrders);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,joinLocalExchange);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        RJNJCJOJoin->addProbe(probeLocalExchange);
        RJNJCJOJoin->addBuild(buildExchange);

        return RJNJCJOJoin;

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

        vector<FieldDesc> RJNBuildSchema = {FieldDesc("n_nationkey","int64")};

        vector<FieldDesc> RJNBuildOutputSchema = {FieldDesc("n_nationkey","int64")};

        vector<int> customerProbeOutputChannels = {0};
        vector<int> customerProbeHashChannels = {3};
        vector<int> RJNbuildOutputChannels = {0};
        vector<int> RJNbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(customerProbeSchema,customerProbeHashChannels,customerProbeOutputChannels,RJNBuildSchema,RJNbuildHashChannels,RJNbuildOutputChannels,RJNBuildOutputSchema);
        LookupJoinNode *RJNJCJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(RJN);

        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"3"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanCustomer);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,joinLocalExchange);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        RJNJCJoin->addProbe(probeLocalExchange);
        RJNJCJoin->addBuild(buildExchange);


        return RJNJCJoin;


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

    LookupJoinNode *createRegionJoinNation()
    {


        PlanNode *filteredRegion = this->createTableScanRegionAndFilter();
        PlanNode *tableScanNation = this->createTableScanNation1();


        vector<FieldDesc> nationProbeSchema = {FieldDesc("n_nationkey","int64"),
                                               FieldDesc("n_name","string"),
                                               FieldDesc("n_regionkey","int64"),
                                               FieldDesc("n_comment","string")};


        vector<FieldDesc> fregionBuildSchema = {FieldDesc("r_regionkey","int64"),FieldDesc("r_name","string"),FieldDesc("r_comment","string")};
        vector<FieldDesc> fregionBuildOutputSchema = {FieldDesc("r_regionkey","int64")};
        vector<int> nationprobeOutputChannels = {0};
        vector<int> nationprobeHashChannels = {2};
        vector<int> fregionbuildOutputChannels = {0};
        vector<int> fregionbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(nationProbeSchema,nationprobeHashChannels,nationprobeOutputChannels,fregionBuildSchema,fregionbuildHashChannels,fregionbuildOutputChannels,fregionBuildOutputSchema);
        LookupJoinNode *RNJoin = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        vector<string> outputLayout;
        vector<string> hashColumnProbe = {"2"};
        vector<string> hashColumnBuild = {"0"};
        vector<int> bucketToPartition;

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"),{}),outputLayout,hashColumnProbe,bucketToPartition);
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanNation);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"),{}),outputLayout,hashColumnBuild,bucketToPartition);
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,filteredRegion);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        RNJoin->addProbe(probeLocalExchange);
        RNJoin->addBuild(buildExchange);


        return RNJoin;


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



    TableScanNode *createTableScanLineitem()
    {
        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));
        return tableScanLineitem;

    }

    TableScanNode *createTableScanPartAndFilter()
    {

        TableScanNode *tableScanPart = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","part"));


        StringLiteral *p_typeCompare = new StringLiteral("0","LARGE BRUSHED BRASS");
        Column *p_type = new Column("0","p_type","string");

        FunctionCall *equal  = new FunctionCall("0","equal","bool");
        equal->addChilds({p_type,p_typeCompare});


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

        return tableScanPart;

    }

    TableScanNode *createTableScanNation1() {
        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "nation"));
        return tableScanNation;
    }

    TableScanNode *createTableScanNation2() {
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






#endif //OLVP_QUERY8_HASH_HPP
