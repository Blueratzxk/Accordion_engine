//
// Created by zxk on 5/15/24.
//

#ifndef OLVP_QUERY12_NL_HPP
#define OLVP_QUERY12_NL_HPP


/*
select
lineitem.shipmode,
sum(case
when orders.orderpriority ='1-URGENT'
or orders.orderpriority ='2-HIGH'
then 1
else 0
end) as high_line_count,
sum(case
when orders.orderpriority <> '1-URGENT'
and orders.orderpriority <> '2-HIGH'
then 1
else 0
end) as low_line_count
from
orders,lineitem
where
orders.orderkey = lineitem.orderkey
and lineitem.shipmode in ('[SHIPMODE1]', '[SHIPMODE2]')
and lineitem.commitdate < lineitem.receiptdate
and lineitem.shipdate < lineitem.commitdate
and lineitem.receiptdate >= date '1995-01-02'
and lineitem.receiptdate < date '1997-01-01' + interval '1' year
group by
lineitem.shipmode
order by
lineitem.shipmode;
*/


#include "../../Query/RegQuery.h"


class Query12_NL:public RegQuery
{

public:
    Query12_NL(){

    }
    string getSql()  {return TpchSqls::Q12();}
    PlanNode* getPlanTree()
    {


        PlanNode *node = createProjectNode();
        PlanNode *pagg = createPartialAgg();
        pagg->addSource(node);

        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(pagg);

        shared_ptr<PartitioningScheme> scheme1 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme1,pagg);


        PlanNode *fagg = createFinalAgg();
        fagg->addSource(exchange);


        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(fagg);
        return (PlanNode*)output;
    }


    PartialAggregationNode *createPartialAgg()
    {



        AggregationDesc partialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"high_line_count",/*outputName=*/"partial_high_line_count"),
                                        AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"low_line_count",/*outputName=*/"partial_low_line_count")},
                /*groupByKeys=*/{"l_shipmode"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),partialAggDesc);

        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_high_line_count",/*outputName=*/"high_line_count"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_low_line_count",/*outputName=*/"low_line_count")},
                /*groupByKeys=*/{"l_shipmode"});

        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);

        return finalAggregationNode;

    }



    ProjectNode *createProjectNode()
    {

        Int64Literal *result1 = new Int64Literal("0","1");
        Int64Literal *result0 = new Int64Literal("0","0");

        IfExpression *conditionIf1 = new IfExpression("0","int64");
        IfExpression *conditionIf11 = new IfExpression("0","int64");

        IfExpression *conditionIf2 = new IfExpression("0","int64");
        IfExpression *conditionIf22 = new IfExpression("0","int64");

        Column *o_orderpriority  = new Column("0","o_orderpriority","string");


        StringLiteral *o_orderpriorityString1 = new StringLiteral("0","1-URGENT");
        StringLiteral *o_orderpriorityString2 = new StringLiteral("0","2-HIGH");


        FunctionCall *o_orderpriorityEqual1 = new FunctionCall("0","equal","bool");
        o_orderpriorityEqual1->addChilds({o_orderpriority,o_orderpriorityString1});

        FunctionCall *o_orderpriorityEqual2 = new FunctionCall("0","equal","bool");
        o_orderpriorityEqual2->addChilds({o_orderpriority,o_orderpriorityString2});


        conditionIf11->setIfBody(o_orderpriorityEqual2,result1,result0);
        conditionIf1->setIfBody(o_orderpriorityEqual1,result1,conditionIf11);




        FunctionCall *o_orderpriorityNotEqual1 = new FunctionCall("0","not_equal","bool");
        o_orderpriorityNotEqual1->addChilds({o_orderpriority,o_orderpriorityString1});

        FunctionCall *o_orderpriorityNotEqual2 = new FunctionCall("0","not_equal","bool");
        o_orderpriorityNotEqual2->addChilds({o_orderpriority,o_orderpriorityString2});


        conditionIf22->setIfBody(o_orderpriorityNotEqual2,result1,result0);
        conditionIf2->setIfBody(o_orderpriorityNotEqual1,conditionIf22,result0);


        ProjectAssignments assignments;


        //FieldDesc("l_shipmode","string"),FieldDesc("o_orderpriority","string")

        assignments.addAssignment(FieldDesc("l_shipmode","string"),FieldDesc("l_shipmode","string"),NULL);
        assignments.addAssignment(FieldDesc("o_orderpriority","string"),FieldDesc("high_line_count","int64"),conditionIf1);
        assignments.addAssignment(FieldDesc("o_orderpriority","string"),FieldDesc("low_line_count","int64"),conditionIf2);


        PlanNode *join = createOrdersJoinFilterLineitem();
        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        projectNode->addSource(join);

        return projectNode;


    }



    LookupJoinNode *createOrdersJoinFilterLineitem()
    {

        PlanNode *fLineitem = this->createTableScanLineitemAndProjFilter();
        PlanNode *tableScanOrders = this->createTableScanOrdersAndProj();



        vector<FieldDesc> fLineitemProbeSchema = {FieldDesc("l_orderkey","int64"),
                                                  FieldDesc("l_shipmode","string")};

        //FieldDesc("l_shipmode","string"),FieldDesc("o_orderpriority","string")

        vector<FieldDesc> ordersBuildSchema =  {FieldDesc("o_orderkey","int64"),
                                                FieldDesc("o_orderpriority","string")};


        vector<FieldDesc> ordersBuildOutputSchema = {FieldDesc("o_orderpriority","string")};

        vector<int> fLineitemProbeOutputChannels = {1};
        vector<int> fLineitemProbeHashChannels = {0};
        vector<int> ordersbuildOutputChannels = {1};
        vector<int> ordersbuildHashChannels = {0};
        LookupJoinDescriptor RNLookupJoinDescriptor(fLineitemProbeSchema,fLineitemProbeHashChannels,fLineitemProbeOutputChannels,ordersBuildSchema,ordersbuildHashChannels,ordersbuildOutputChannels,ordersBuildOutputSchema);
        LookupJoinNode *lookupJoinNode = new LookupJoinNode(UUID::create_uuid(),RNLookupJoinDescriptor);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,fLineitem);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanOrders);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);

        lookupJoinNode->addProbe(probeExchange);
        lookupJoinNode->addBuild(buildExchange);




        return lookupJoinNode;


    }





    ProjectNode *createTableScanOrdersAndProj()
    {
        TableScanNode *tableScanOrders = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","orders"));

        ProjectAssignments assignments;
        assignments.addAssignment(FieldDesc("o_orderkey","int64"),FieldDesc("o_orderkey","int64"),NULL);
        assignments.addAssignment(FieldDesc("o_custkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderstatus","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_totalprice","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_orderpriority","string"),FieldDesc("o_orderpriority","string"),NULL);
        assignments.addAssignment(FieldDesc("o_clerk","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_shippriority","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("o_comment","string"),FieldDesc::getEmptyDesc(),NULL);



        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        projectNode->addSource(tableScanOrders);

        return  projectNode;


    }



    ProjectNode *createTableScanLineitemAndProjFilter()
    {

        Column *l_shipmode = new Column("0","l_shipmode","string");
        InExpression *shipmodeIn = new InExpression("0","string",{"AIR","TRUCK"});
        shipmodeIn->addChilds({l_shipmode});

        //===================================//


        Column *l_commitdate = new Column("0","l_commitdate","date32");
        Column *l_shipdate = new Column("0","l_shipdate","date32");
        Column *l_receiptdate = new Column("0","l_receiptdate","date32");

        FunctionCall *lcomLessthanlrec = new FunctionCall("0","less_than","bool");
        lcomLessthanlrec->addChilds({l_commitdate,l_receiptdate});

        FunctionCall *lshipLessthanlcom = new FunctionCall("0","less_than","bool");
        lshipLessthanlcom->addChilds({l_shipdate,l_commitdate});

//===================================//


        Date32Literal *date32Literalup = new Date32Literal("0","1995-09-21");

        FunctionCall *castLineitemDateToDate64 = new FunctionCall("0","castDATE","date64");
        castLineitemDateToDate64->addChilds({l_receiptdate});


        FunctionCall *castUp = new FunctionCall("0","castDATE","date32");
        castUp->addChilds({date32Literalup});
        FunctionCall *castDate32LiteralupToDate64 = new FunctionCall("0","castDATE","date64");
        castDate32LiteralupToDate64->addChilds({castUp});

        Int32Literal *addOne = new Int32Literal("0","1");
        FunctionCall *addYear = new FunctionCall("0","timestampaddYear","date64");
        addYear->addChilds({castDate32LiteralupToDate64,addOne});



        FunctionCall *l_shipdate_lessthan = new FunctionCall("0","less_than","bool");
        l_shipdate_lessthan->addChilds({castLineitemDateToDate64,addYear});


        Date32Literal *date32Literaldown = new Date32Literal("0","1995-03-21");
        FunctionCall *castLineitemDate2 = new FunctionCall("0","castDATE","date32");
        castLineitemDate2->addChilds({date32Literaldown});

        FunctionCall *l_shipdate_greaterequal = new FunctionCall("0","greater_than_or_equal_to","bool");
        l_shipdate_greaterequal->addChilds({l_receiptdate,castLineitemDate2});

        //===================================//

        FunctionCall *and1 = new FunctionCall("0","and","bool");
        FunctionCall *and2 = new FunctionCall("0","and","bool");
        FunctionCall *and3 = new FunctionCall("0","and","bool");
        FunctionCall *and4 = new FunctionCall("0","and","bool");

        and1->addChilds({lcomLessthanlrec,lshipLessthanlcom});
        and2->addChilds({l_shipdate_lessthan,l_shipdate_greaterequal});
        and3->addChilds({and1,and2});
        and4->addChilds({and3,shipmodeIn});



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

        FilterDescriptor filterDescriptor(fieldsIn,and4);
        FilterNode *filterlineitem = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterlineitem->addSource(tableScanLineitem);






        ProjectAssignments assignments;


        //FieldDesc("ps_partkey","int64"),FieldDesc("ps_availqty","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("s_suppkey","int64")

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
        assignments.addAssignment(FieldDesc("l_shipmode","string"),FieldDesc("l_shipmode","string"),NULL);
        assignments.addAssignment(FieldDesc("l_comment","string"),FieldDesc::getEmptyDesc(),NULL);


        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        projectNode->addSource(filterlineitem);





        return projectNode;









    }
};



#endif //OLVP_QUERY12_NL_HPP
