//
// Created by zxk on 11/28/23.
//

#ifndef OLVP_QUERY10_2_HPP
#define OLVP_QUERY10_2_HPP





#include "../../Query/RegQuery.h"


class Query10_2:public RegQuery
{

public:
    Query10_2(){

    }
    string getSql()  {return TpchSqls::Q10();}
    PlanNode* getPlanTree()
    {


        PlanNode *join = create4TJ();
        PlanNode *proj = createProjectNode();
        proj->addSource(join);

        PlanNode *pagg = createPartialAgg();
        pagg->addSource(proj);


        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(pagg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,paggLocalExchange);


        PlanNode *fagg = createFinalAgg();
        fagg->addSource(exchange);

        //  vector<string> sortKeys = {"revenue"};
        //   vector<SortOperator::SortOrder> sortOrders = {SortOperator::Descending};
        // //   SortDescriptor sortDescriptor(sortKeys,sortOrders);
        //   SortNode *sort = new SortNode(UUID::create_uuid(),sortDescriptor);
        //   sort->addSource(fagg);


        vector<string> sortKeys = {"revenue"};
        vector<TopKOperator::SortOrder> sortOrders = {TopKOperator::Descending};
        TopKDescriptor topkDescriptor(100,sortKeys,sortOrders);
        TopKNode *topk = new TopKNode(UUID::create_uuid(),topkDescriptor);
        topk->addSource(fagg);


        //   LimitNode *limit = new LimitNode(UUID::create_uuid(),10);
        //    limit->addSource(sort);

        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(topk);
        return (PlanNode*)output;
    }




    PartialAggregationNode *createPartialAgg()
    {



        AggregationDesc partialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"revenue",/*outputName=*/"partial_revenue")},
                /*groupByKeys=*/{"c_custkey","c_name","c_acctbal","c_phone","n_name","c_address","c_comment"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),partialAggDesc);

        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_revenue",/*outputName=*/"revenue")},
                /*groupByKeys=*/{"c_custkey","c_name","c_acctbal","c_phone","n_name","c_address","c_comment"});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);

        return finalAggregationNode;

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




        ProjectAssignments assignments;


        //FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("c_custkey","int64"),
        //                                            FieldDesc("c_name","string"),
        //                                            FieldDesc("c_address","string"),
        //                                            FieldDesc("c_phone","string"),
        //                                            FieldDesc("c_acctbal","double"),
        //                                            FieldDesc("c_comment","string"),
        //                                            FieldDesc("n_name","string")

        //l_extendedprice * (1 - l_discount)

        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("revenue","double"),disc_price);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("c_custkey","int64"),FieldDesc("c_custkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("c_name","string"),FieldDesc("c_name","string"),NULL);
        assignments.addAssignment(FieldDesc("c_address","string"),FieldDesc("c_address","string"),NULL);
        assignments.addAssignment(FieldDesc("c_phone","string"),FieldDesc("c_phone","string"),NULL);
        assignments.addAssignment(FieldDesc("c_acctbal","double"),FieldDesc("c_acctbal","double"),NULL);
        assignments.addAssignment(FieldDesc("c_comment","string"),FieldDesc("c_comment","string"),NULL);
        assignments.addAssignment(FieldDesc("n_name","string"),FieldDesc("n_name","string"),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }

    LookupJoinNode *create4TJ()
    {

        PlanNode *FLJFO = this->createFilteredLineitemJoinFilteredOrders();
        PlanNode *CJN = this->createCustomerJoinNation();


        vector<FieldDesc> FLJFOProbeSchema = {FieldDesc("l_extendedprice","double"),
                                              FieldDesc("l_discount","double"),
                                              FieldDesc("o_custkey","int64")};

        //FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("c_custkey","int64"),
        //                                            FieldDesc("c_name","string"),
        //                                            FieldDesc("c_address","string"),
        //                                            FieldDesc("c_phone","string"),
        //                                            FieldDesc("c_acctbal","double"),
        //                                            FieldDesc("c_comment","string"),
        //                                            FieldDesc("n_name","string")
        vector<FieldDesc> CJNBuildSchema = {FieldDesc("c_custkey","int64"),
                                            FieldDesc("c_name","string"),
                                            FieldDesc("c_address","string"),
                                            FieldDesc("c_phone","string"),
                                            FieldDesc("c_acctbal","double"),
                                            FieldDesc("c_comment","string"),
                                            FieldDesc("n_name","string")};

        vector<FieldDesc> CJNBuildOutputSchema = {FieldDesc("c_custkey","int64"),
                                                  FieldDesc("c_name","string"),
                                                  FieldDesc("c_address","string"),
                                                  FieldDesc("c_phone","string"),
                                                  FieldDesc("c_acctbal","double"),
                                                  FieldDesc("c_comment","string"),
                                                  FieldDesc("n_name","string")};

        vector<int> FLJFOProbeOutputChannels = {0,1};
        vector<int> FLJFOProbeHashChannels = {2};
        vector<int> CJNbuildOutputChannels = {0,1,2,3,4,5,6};
        vector<int> CJNbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(FLJFOProbeSchema,FLJFOProbeHashChannels,FLJFOProbeOutputChannels,CJNBuildSchema,CJNbuildHashChannels,CJNbuildOutputChannels,CJNBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

     //   LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
     //   joinLocalExchange->addSource(FLJFO);

        LocalExchangeNode *joinLocalExchange2 = new LocalExchangeNode("joinLocalExchange2");
        joinLocalExchange2->addSource(CJN);


     //   shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
     //   ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,joinLocalExchange);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,joinLocalExchange2);

    //    LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
    //    probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(FLJFO);
        lookupJoinNode->addBuild(buildExchange);


        return lookupJoinNode;

    }


    LookupJoinNode *createCustomerJoinNation()
    {

        PlanNode *customer = this->createTableScanCustomer();
        PlanNode *nation = this->createTableScanNation();


        vector<FieldDesc> customerProbeSchema = {FieldDesc("c_custkey","int64"),
                                                 FieldDesc("c_name","string"),
                                                 FieldDesc("c_address","string"),
                                                 FieldDesc("c_nationkey","int64"),
                                                 FieldDesc("c_phone","string"),
                                                 FieldDesc("c_acctbal","double"),
                                                 FieldDesc("c_mktsegment","string"),
                                                 FieldDesc("c_comment","string")};

        //  FieldDesc("c_custkey","int64"),FieldDesc("c_name","string"),FieldDesc("c_address","string"),FieldDesc("c_phone","string"),FieldDesc("c_acctbal","double"),FieldDesc("c_comment","string"),FieldDesc("n_name","string")

        vector<FieldDesc> nationBuildSchema = {FieldDesc("n_nationkey","int64"),
                                               FieldDesc("n_name","string"),
                                               FieldDesc("n_regionkey","int64"),
                                               FieldDesc("n_comment","string")};

        vector<FieldDesc> nationBuildOutputSchema = {FieldDesc("n_name","string")};

        vector<int> customerProbeOutputChannels = {0,1,2,4,5,7};
        vector<int> customerProbeHashChannels = {3};
        vector<int> nationbuildOutputChannels = {1};
        vector<int> nationbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(customerProbeSchema,customerProbeHashChannels,customerProbeOutputChannels,nationBuildSchema,nationbuildHashChannels,nationbuildOutputChannels,nationBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,customer);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,nation);


        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(probeLocalExchange);
        lookupJoinNode->addBuild(buildExchange);


        return lookupJoinNode;
    }


    LookupJoinNode *createFilteredLineitemJoinFilteredOrders()
    {


        PlanNode *flineitem = this->createTableScanLineitemAndFilter();
        PlanNode *forders = this->createOrdersTableScanAndFilter();


        vector<FieldDesc> flineitemProbeSchema = {FieldDesc("l_orderkey","int64"),
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

        //  FieldDesc("l_extendedprice","double"),FieldDesc("l_discount","double"),FieldDesc("o_custkey","int64")

        vector<FieldDesc> fordersBuildSchema = {FieldDesc("o_orderkey","int64"),
                                                FieldDesc("o_custkey","int64"),
                                                FieldDesc("o_orderstatus","string"),
                                                FieldDesc("o_totalprice","double"),
                                                FieldDesc("o_orderdate","date32"),
                                                FieldDesc("o_orderpriority","string"),
                                                FieldDesc("o_clerk","string"),
                                                FieldDesc("o_shippriority","int64"),
                                                FieldDesc("o_comment","string")};

        vector<FieldDesc> fordersBuildOutputSchema = {FieldDesc("o_custkey","int64")};

        vector<int> flineitemProbeOutputChannels = {5,6};
        vector<int> flineitemProbeHashChannels = {0};
        vector<int> fordersbuildOutputChannels = {1};
        vector<int> fordersbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(flineitemProbeSchema,flineitemProbeHashChannels,flineitemProbeOutputChannels,fordersBuildSchema,fordersbuildHashChannels,fordersbuildOutputChannels,fordersBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,flineitem);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,forders);


        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(probeLocalExchange);
        lookupJoinNode->addBuild(buildExchange);


        return lookupJoinNode;


    }


    FilterNode *createOrdersTableScanAndFilter()
    {


        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        Column *o_orderdate = new Column("0","o_orderdate","date32");

        Date32Literal *date32Literalup = new Date32Literal("0","1996-12-31");


        FunctionCall *castUpDate32 = new FunctionCall("0","castDATE","date32");
        castUpDate32->addChilds({date32Literalup});
        FunctionCall *castUpDate64 = new FunctionCall("0","castDATE","date64");
        castUpDate64->addChilds({castUpDate32});



        Int32Literal *monthAddValue = new Int32Literal("0","3");

        FunctionCall *addMonth = new FunctionCall("0","timestampaddMonth","date64");

        addMonth->addChilds({castUpDate64,monthAddValue});

        FunctionCall *castorderdate64 = new FunctionCall("0","castDATE","date64");
        castorderdate64->addChilds({o_orderdate});

        FunctionCall *o_orderdate_lessthan = new FunctionCall("0","less_than","bool");
        o_orderdate_lessthan->addChilds({castorderdate64,addMonth});


        Date32Literal *date32Literaldown = new Date32Literal("0","1995-01-01");
        FunctionCall *castDown = new FunctionCall("0","castDATE","date32");
        castDown->addChilds({date32Literaldown});

        FunctionCall *o_orderdate_greaterequal = new FunctionCall("0","greater_than_or_equal_to","bool");
        o_orderdate_greaterequal->addChilds({o_orderdate,castDown});


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






    FilterNode *createTableScanLineitemAndFilter()
    {

        Column *l_returnflag = new Column("0","l_returnflag","string");

        StringLiteral *flag = new StringLiteral("0","R");


        FunctionCall *equal = new FunctionCall("0","equal","bool");
        equal->addChilds({l_returnflag,flag});

        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));


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

        FilterDescriptor filterDescriptor(fieldsIn,equal);
        FilterNode *filterlineitem = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterlineitem->addSource(tableScanLineitem);
        return filterlineitem;



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


};


#endif //OLVP_QUERY10_2_HPP
