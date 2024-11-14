//
// Created by zxk on 6/28/24.
//

#ifndef OLVP_QUERY4_NL_MULHASHBUILD_HPP
#define OLVP_QUERY4_NL_MULHASHBUILD_HPP




/*

 select
o_orderpriority, //订单优先级
count(*) as order_count //订单优先级计数
from orders //单表查询
where
o_orderdate >= date '[DATE]'
and o_orderdate < date '[DATE]' + interval '3' month //指定订单的时间段--某三个月，DATE是在1993年1月和1997年10月之间随机选择的一个月的第一天
and exists ( //子查询
select
*
from
lineitem
where
l_orderkey = o_orderkey
and l_commitdate < l_receiptdate
)
group by //按订单优先级分组
o_orderpriority
order by //按订单优先级排序
o_orderpriority;

 * */


#include "../../Query/RegQuery.h"

class Query4_NL_MulHashBuild:public RegQuery
{

public:
    Query4_NL_MulHashBuild(){

    }
    string getSql()  {return TpchSqls::Q4();}
    PlanNode* getPlanTree()
    {


        PlanNode *lineitem = createLineitemTableScanAndFilter();

        shared_ptr<PartitioningScheme> schemeTableScan = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *tableScanExchange = new ExchangeNode("tableScanExchange",ExchangeNode::REPARTITION,schemeTableScan,lineitem);


        LocalExchangeNode *paggLocalExchange1 = new LocalExchangeNode("paggLocalExchange1");
        paggLocalExchange1->addSource(tableScanExchange);

        PlanNode *partialAggLineitem = createFilteredLineitemPartialAgg();
        partialAggLineitem->addSource(tableScanExchange);

        LocalExchangeNode *paggLocalExchange2 = new LocalExchangeNode("paggLocalExchange2");
        paggLocalExchange2->addSource(partialAggLineitem);

        shared_ptr<PartitioningScheme> partialAggExchangeScheme = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *partialAggExchange = new ExchangeNode("partialAggExchange",ExchangeNode::GATHER,partialAggExchangeScheme,partialAggLineitem);

        FinalAggregationNode *finalAggLineitem = createFilterLineitemFinalAgg();
        finalAggLineitem->addSource(partialAggExchange);



        LookupJoinNode *twoTJ = createOrdersJoinFinalAggLineitem(finalAggLineitem);

        PlanNode *joinPartialAgg = create2TJPartialAgg();
        joinPartialAgg->addSource(twoTJ);

        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(joinPartialAgg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,joinPartialAgg);


        PlanNode *joinFinalAgg = create2TJFinalAgg();
        joinFinalAgg->addSource(exchange);

        vector<string> sortKeys = {"o_orderpriority"};
        vector<SortOperator::SortOrder> sortOrders = {SortOperator::Ascending};
        SortDescriptor sortDescriptor(sortKeys,sortOrders);
        SortNode *sort = new SortNode(UUID::create_uuid(),sortDescriptor);
        sort->addSource(joinFinalAgg);



        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(sort);
        return (PlanNode*)output;
    }

    FilterNode *createLineitemTableScanAndFilter()
    {

        //l_commitdate < l_receiptdate

        TableScanNode *tableScanLineitem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));

        Column *l_commitdate = new Column("0","l_commitdate","date32");
        Column *l_receiptdate = new Column("0","l_receiptdate","date32");


        FunctionCall *l_commitdate_lessthan_l_receiptdate = new FunctionCall("0","less_than","bool");
        l_commitdate_lessthan_l_receiptdate->addChilds({l_commitdate,l_receiptdate});



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

        FilterDescriptor filterDescriptor(fieldsIn,l_commitdate_lessthan_l_receiptdate);
        FilterNode *filterLineitem = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterLineitem->addSource(tableScanLineitem);
        return filterLineitem;

    }

    FilterNode *createOrdersTableScanAndFilter()
    {


        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        Column *o_orderdate = new Column("0","o_orderdate","date32");

        Date32Literal *date32Literalup = new Date32Literal("0","1995-09-21");
        FunctionCall *castOrderDate = new FunctionCall("0","castDATE","date32");
        castOrderDate->addChilds({date32Literalup});

        Date32Literal *date32Literaldown = new Date32Literal("0","1995-03-21");
        FunctionCall *castOrderDate2 = new FunctionCall("0","castDATE","date32");
        castOrderDate2->addChilds({date32Literaldown});

        FunctionCall *o_orderdate_lessthan = new FunctionCall("0","less_than","bool");
        o_orderdate_lessthan->addChilds({o_orderdate,castOrderDate});

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



    LookupJoinNode *createOrdersJoinFinalAggLineitem(FinalAggregationNode *finalAggLineitem)
    {

        PlanNode *forders = createOrdersTableScanAndFilter();

        vector<FieldDesc> fordersProbeSchema = {FieldDesc("o_orderkey","int64"),
                                                FieldDesc("o_custkey","int64"),
                                                FieldDesc("o_orderstatus","string"),
                                                FieldDesc("o_totalprice","double"),
                                                FieldDesc("o_orderdate","date32"),
                                                FieldDesc("o_orderpriority","string"),
                                                FieldDesc("o_clerk","string"),
                                                FieldDesc("o_shippriority","int64"),
                                                FieldDesc("o_comment","string")};

        vector<FieldDesc> finalAggLineitemBuildSchema = {FieldDesc("l_orderkey","int64"),
                                                         FieldDesc("count_l_orderkey","int64")};



        vector<FieldDesc> finalAggLineitemBuildOutputSchema = {FieldDesc("count_l_orderkey","int64")};

        vector<int> fOrdersprobeOutputChannels = {0,5};
        vector<int> fOrdersprobeHashChannels = {0};
        vector<int> finalAggLineitembuildOutputChannels = {1};
        vector<int> finalAggLineitembuildHashChannels = {0};
        LookupJoinDescriptor twoTJ_LookupJoinDescriptor(fordersProbeSchema,fOrdersprobeHashChannels,fOrdersprobeOutputChannels,finalAggLineitemBuildSchema,finalAggLineitembuildHashChannels,finalAggLineitembuildOutputChannels,finalAggLineitemBuildOutputSchema);
        LookupJoinNode *fOrdersFinalAggLineitemJoin = new LookupJoinNode(UUID::create_uuid(),twoTJ_LookupJoinDescriptor);

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,forders);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,finalAggLineitem);

        LocalExchangeNode *tableScanLocalExchange = new LocalExchangeNode("tableScanLocalExchange");
        tableScanLocalExchange->addSource(probeExchange);

        LocalExchangeNode *hashexchange = new LocalExchangeNode("LocalExchange","hash",{0});
        hashexchange->addSource(buildExchange);

        fOrdersFinalAggLineitemJoin->addProbe(probeExchange);
        fOrdersFinalAggLineitemJoin->addBuild(hashexchange);


        return fOrdersFinalAggLineitemJoin;



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

    PartialAggregationNode *createFilteredLineitemPartialAgg()
    {


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"hash_count_all",/*inputKey=*/"",/*outputName=*/"partial_count_l_orderkey")},
                /*groupByKeys=*/{"l_orderkey"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFilterLineitemFinalAgg()
    {


        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_count_l_orderkey",/*outputName=*/"count_l_orderkey")},
                /*groupByKeys=*/{"l_orderkey"});



        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }



    PartialAggregationNode *create2TJPartialAgg()
    {


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"count_l_orderkey",/*outputName=*/"partial_sum_l_orderkey")},
                /*groupByKeys=*/{"o_orderpriority"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *create2TJFinalAgg()
    {


        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_sum_l_orderkey",/*outputName=*/"order_count")},
                /*groupByKeys=*/{"o_orderpriority"});



        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }


};





#endif //OLVP_QUERY4_NL_MULHASHBUILD_HPP
