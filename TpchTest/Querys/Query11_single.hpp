//
// Created by zxk on 5/28/24.
//

#ifndef OLVP_QUERY11_SINGLE_HPP
#define OLVP_QUERY11_SINGLE_HPP


/*
select
c_custkey, c_name, //客户信息
sum(l_extendedprice * (1 - l_discount)) as revenue, //收入损失
c_acctbal,
n_name, c_address, c_phone, c_comment //国家、地址、电话、意见信息等
from
customer, orders, lineitem, nation
where
c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate >= date '[DATE]' // DATE是位于1993年一月到1994年十二月中任一月的一号
and o_orderdate < date '[DATE]' + interval '3' month //3个月内
and l_returnflag = 'R' //货物被回退
and c_nationkey = n_nationkey
group by
c_custkey,
c_name,
c_acctbal,
c_phone,
n_name,
c_address,
c_comment
order by
revenue desc;

 * */



#include "../../Query/RegQuery.h"


class Query11_single:public RegQuery
{

public:
    Query11_single(){

    }
    string getSql()  {return TpchSqls::Q11();}
    PlanNode* getPlanTree()
    {


        PlanNode *node = createFinalFilter();

        LocalExchangeNode *localExchange = new LocalExchangeNode("localExchange");
        localExchange->addSource(node);

        shared_ptr<PartitioningScheme> schemeExchange = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("schemeExchange",ExchangeNode::GATHER,schemeExchange,node);

        vector<string> sortKeys = {"value"};
        vector<TopKOperator::SortOrder> sortOrders = {TopKOperator::Descending};
        TopKDescriptor topkDescriptor(100,sortKeys,sortOrders);
        TopKNode *topk = new TopKNode(UUID::create_uuid(),topkDescriptor);
        topk->addSource(exchange);


        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(topk);
        return (PlanNode*)output;
    }


    PlanNode *createFinalFilter()
    {

        Column *value_subQ = new Column("0","value_subQ","double");
        Column *value = new Column("0","value","double");

        FunctionCall *greaterThan = new FunctionCall("0","greater_than","bool");
        greaterThan->addChilds({value,value_subQ});


        PlanNode *crossJoin = createCrossjoin();
        vector<FieldDesc> fieldsIn = {FieldDesc("value_subQ","double"),
                                      FieldDesc("ps_partkey","int64"),
                                      FieldDesc("value","double")};

        //value_subQ	ps_partkey	value

        FilterDescriptor filterDescriptor(fieldsIn,greaterThan);
        FilterNode *filter = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filter->addSource(crossJoin);




        ProjectAssignments assignments;


        //FieldDesc("ps_partkey","int64"),FieldDesc("ps_availqty","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("s_suppkey","int64")

        assignments.addAssignment(FieldDesc("value_subQ","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("ps_partkey","int64"),FieldDesc("ps_partkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("value","double"),FieldDesc("value","double"),NULL);



        ProjectNode *project = new ProjectNode(UUID::create_uuid(),assignments);
        project->addSource(filter);
        return project;
    }


    PlanNode *createCrossjoin()
    {
        PlanNode *subquery = this->createFilteredNationJoinSupplierJoinPartsupp();
        PlanNode *mainquery = this->createFilteredNationJoinSupplierJoinPartsupp();

        PlanNode *projectSub = this->createProjectNodeSubQuery();
        projectSub->addSource(subquery);
        PlanNode *project = this->createProjectNode();
        project->addSource(mainquery);

        PlanNode *partialAggSub = this->createPartialAggSubQuery();
        partialAggSub->addSource(projectSub);

        LocalExchangeNode *paggLocalExchange1 = new LocalExchangeNode("paggLocalExchange1");
        paggLocalExchange1->addSource(partialAggSub);

        shared_ptr<PartitioningScheme> scheme1 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange1 = new ExchangeNode("exchange1",ExchangeNode::GATHER,scheme1,partialAggSub);



        PlanNode *partialAgg = this->createPartialAgg();
        partialAgg->addSource(project);

        LocalExchangeNode *paggLocalExchange2 = new LocalExchangeNode("paggLocalExchange2");
        paggLocalExchange2->addSource(partialAgg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange2 = new ExchangeNode("exchange2",ExchangeNode::GATHER,scheme2,partialAgg);



        PlanNode *finalAggSub = this->createFinalAggSubQuery();
        finalAggSub->addSource(exchange1);

        PlanNode *finalAgg = this->createFinalAgg();
        finalAgg->addSource(exchange2);



        vector<FieldDesc> probe = {FieldDesc("value_subQ","double"),};

        vector<FieldDesc> build = {FieldDesc("ps_partkey","int64"),
                                   FieldDesc("value","double")};

        CrossJoinDescriptor desc(probe,build,probe,build);

        CrossJoinNode *crossJoin = new CrossJoinNode(UUID::create_uuid(),desc);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,finalAggSub);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,finalAgg);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);


        crossJoin->addProbe(probeExchange);
        crossJoin->addBuild(buildExchange);

        return crossJoin;
    }

    PartialAggregationNode *createPartialAggSubQuery()
    {



        AggregationDesc partialAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"value",/*outputName=*/"partial_value")},
                /*groupByKeys=*/{});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),partialAggDesc);

        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAggSubQuery()
    {



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"partial_value",/*outputName=*/"value_subQ")},
                /*groupByKeys=*/{});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);

        return finalAggregationNode;

    }


    PartialAggregationNode *createPartialAgg()
    {



        AggregationDesc partialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"value",/*outputName=*/"partial_value")},
                /*groupByKeys=*/{"ps_partkey"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),partialAggDesc);

        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_value",/*outputName=*/"value")},
                /*groupByKeys=*/{"ps_partkey"});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);

        return finalAggregationNode;

    }


    ProjectNode *createProjectNode()
    {


        Column *ps_supplycost = new Column("0","ps_supplycost","double");
        Column *ps_availqty  = new Column("0","ps_availqty","int64");


        FunctionCall *cast_ps_availqty_Float8 = new FunctionCall("0","castFLOAT8","double");
        cast_ps_availqty_Float8->addChilds({ps_availqty});

        FunctionCall *mul = new FunctionCall("0","multiply","double");
        mul->addChilds({cast_ps_availqty_Float8,ps_supplycost});



        ProjectAssignments assignments;


        //FieldDesc("ps_partkey","int64"),FieldDesc("ps_availqty","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("s_suppkey","int64")

        assignments.addAssignment(FieldDesc("ps_partkey","int64"),FieldDesc("ps_partkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("ps_availqty","int64"),FieldDesc("value","double"),mul);
        assignments.addAssignment(FieldDesc("ps_supplycost","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("s_suppkey","int64"),FieldDesc::getEmptyDesc(),NULL);


        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }


    ProjectNode *createProjectNodeSubQuery()
    {


        Column *ps_supplycost = new Column("0","ps_supplycost","double");
        Column *ps_availqty  = new Column("0","ps_availqty","int64");
        DoubleLiteral *faction = new DoubleLiteral("0","0.000001");


        FunctionCall *cast_ps_availqty_Float8 = new FunctionCall("0","castFLOAT8","double");
        cast_ps_availqty_Float8->addChilds({ps_availqty});

        FunctionCall *mul1 = new FunctionCall("0","multiply","double");
        mul1->addChilds({cast_ps_availqty_Float8,ps_supplycost});

        FunctionCall *mul2 = new FunctionCall("0","multiply","double");
        mul2->addChilds({mul1,faction});


        ProjectAssignments assignments;


        //FieldDesc("ps_partkey","int64"),FieldDesc("ps_availqty","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("s_suppkey","int64")

        assignments.addAssignment(FieldDesc("ps_partkey","int64"),FieldDesc("ps_partkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("ps_availqty","int64"),FieldDesc("value","double"),mul2);
        assignments.addAssignment(FieldDesc("ps_supplycost","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("s_suppkey","int64"),FieldDesc::getEmptyDesc(),NULL);


        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }


    LookupJoinNode *createFilteredNationJoinSupplierJoinPartsupp()
    {

        PlanNode *FNJS = this->createFilteredNationJoinSupplier();
        PlanNode *tableScanPartSupp = this->createTableScanPartsupp();



        vector<FieldDesc> partSuppProbeSchema = {FieldDesc("ps_partkey","int64"),
                                                 FieldDesc("ps_suppkey","int64"),
                                                 FieldDesc("ps_availqty","int64"),
                                                 FieldDesc("ps_supplycost","double"),
                                                 FieldDesc("ps_comment","string")};

        // FieldDesc("ps_partkey","int64"),FieldDesc("ps_availqty","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("s_suppkey","int64")

        vector<FieldDesc> FNJSBuildSchema = {FieldDesc("s_suppkey","int64"),
                                             FieldDesc("n_nationkey","int64")};

        vector<FieldDesc> FNJSBuildOutputSchema = {FieldDesc("s_suppkey","int64")};

        vector<int> partSuppProbeOutputChannels = {0,2,3};
        vector<int> partSuppProbeHashChannels = {1};
        vector<int> FNJSbuildOutputChannels = {0};
        vector<int> FNJSbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(partSuppProbeSchema,partSuppProbeHashChannels,partSuppProbeOutputChannels,FNJSBuildSchema,FNJSbuildHashChannels,FNJSbuildOutputChannels,FNJSBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(FNJS);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanPartSupp);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,FNJS);


        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(tableScanPartSupp);
        lookupJoinNode->addBuild(buildExchange);


        return lookupJoinNode;


    }



    LookupJoinNode *createFilteredNationJoinSupplier()
    {

        PlanNode *fnation = this->createTableScanNationAndFilter1();
        PlanNode *tableScanSupplier = this->createTableScanSupplier();



        vector<FieldDesc> supplierProbeSchema = {FieldDesc("s_suppkey","int64"),
                                                 FieldDesc("s_name","string"),
                                                 FieldDesc("s_address","int64"),
                                                 FieldDesc("s_nationkey","int64"),
                                                 FieldDesc("s_phone","string"),
                                                 FieldDesc("s_acctbal","double"),
                                                 FieldDesc("s_comment","string")};

        //FieldDesc("s_suppkey","int64"),FieldDesc("n_nationkey","int64")

        vector<FieldDesc> fnationBuildSchema = {FieldDesc("n_nationkey","int64"),
                                                FieldDesc("n_name","string"),
                                                FieldDesc("n_regionkey","int64"),
                                                FieldDesc("n_comment","string")};

        vector<FieldDesc> fnationBuildOutputSchema = {FieldDesc("n_nationkey","int64")};

        vector<int> supplierProbeOutputChannels = {0};
        vector<int> supplierProbeHashChannels = {3};
        vector<int> fnationbuildOutputChannels = {0};
        vector<int> fnationbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(supplierProbeSchema,supplierProbeHashChannels,supplierProbeOutputChannels,fnationBuildSchema,fnationbuildHashChannels,fnationbuildOutputChannels,fnationBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);



        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange= new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanSupplier);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,fnation);


        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(probeExchange);
        lookupJoinNode->addBuild(buildExchange);


        return lookupJoinNode;


    }






    FilterNode *createTableScanNationAndFilter1() {

        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "nation"));


        Column *n_name = new Column("0","n_name","string");
        StringLiteral *n_nameValue = new StringLiteral("0","FRANCE");

        FunctionCall *name_equal = new FunctionCall("0","equal","bool");
        name_equal->addChilds({n_name,n_nameValue});



        vector<FieldDesc> fieldsIn = {FieldDesc("n_nationkey","int64"),
                                      FieldDesc("n_name","string"),
                                      FieldDesc("n_regionkey","int64"),
                                      FieldDesc("n_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,name_equal);
        FilterNode *filterNation = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterNation->addSource(tableScanNation);
        return filterNation;

    }



    FilterNode *createTableScanNationAndFilter2() {

        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),
                                                           TableScanDescriptor("tpch_test", "tpch_1", "nation"));


        Column *n_name = new Column("0","n_name","string");
        StringLiteral *n_nameValue = new StringLiteral("0","FRANCE");

        FunctionCall *name_equal = new FunctionCall("0","equal","bool");
        name_equal->addChilds({n_name,n_nameValue});



        vector<FieldDesc> fieldsIn = {FieldDesc("n_nationkey","int64"),
                                      FieldDesc("n_name","string"),
                                      FieldDesc("n_regionkey","int64"),
                                      FieldDesc("n_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,name_equal);
        FilterNode *filterNation = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterNation->addSource(tableScanNation);
        return filterNation;

    }


    TableScanNode *createTableScanSupplier() {
        TableScanNode *tableScanSupplier = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "supplier"));

        return tableScanSupplier;
    }


    TableScanNode *createTableScanPartsupp() {
        TableScanNode *tableScanPartsupp = new TableScanNode(UUID::create_uuid(),
                                                             TableScanDescriptor("tpch_test", "tpch_1", "partsupp"));

        return tableScanPartsupp;
    }

};



#endif //OLVP_QUERY11_SINGLE_HPP
