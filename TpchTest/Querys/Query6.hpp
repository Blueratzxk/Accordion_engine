//
// Created by zxk on 6/25/23.
//

#ifndef OLVP_QUERY6_HPP
#define OLVP_QUERY6_HPP


#include "../../Query/RegQuery.h"
/*
select
sum(l_extendedprice*l_discount) as revenue //潜在的收入增加量
from
lineitem //单表查询
where
l_shipdate >= date '[DATE]' //DATE是从[1993, 1997]中随机选择的一年的1月1日
and l_shipdate < date '[DATE]' + interval '1' year //一年内
and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01 //between
and l_quantity < [QUANTITY]; // QUANTITY在区间[24, 25]中随机选择
*/


class Query6:public RegQuery
{

public:
    Query6(){

    }
    string getSql()  {return TpchSqls::Q6();}
    PlanNode* getPlanTree()
    {
        TableScanNode *tableScanLineItem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));


        PlanNode *filter = createFilterNode();
        filter->addSource(tableScanLineItem);

        PlanNode *project = createProject();
        project->addSource(filter);

        shared_ptr<PartitioningScheme> schemeTableScan = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *tableScanExchange = new ExchangeNode("tableScanExchange",ExchangeNode::REPARTITION,schemeTableScan,project);

        LocalExchangeNode *paggLocalExchange1 = new LocalExchangeNode("paggLocalExchange1");
        paggLocalExchange1->addSource(tableScanExchange);

        PlanNode *partialAgg = createPartialAgg();
        partialAgg->addSource(paggLocalExchange1);

        LocalExchangeNode *paggLocalExchange2 = new LocalExchangeNode("paggLocalExchange2");
        paggLocalExchange2->addSource(partialAgg);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,paggLocalExchange2);


        PlanNode *finalAgg = createFinalAgg();
        finalAgg->addSource(exchange);



        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(finalAgg);
        return (PlanNode*)output;
    }

    FilterNode *createFilterNode()
    {


        /*
         l_shipdate >= date '[DATE]' //DATE是从[1993, 1997]中随机选择的一年的1月1日
            and l_shipdate < date '[DATE]' + interval '1' year //一年内
            and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01 //between
            and l_quantity < [QUANTITY]; // QUANTITY在区间[24, 25]中随机选择
         * */


        Column *l_shipdate = new Column("0","l_shipdate","date32");

        Date32Literal *date32Literalup = new Date32Literal("0","1995-09-21");

        FunctionCall *castLineitemDateToDate64 = new FunctionCall("0","castDATE","date64");
        castLineitemDateToDate64->addChilds({l_shipdate});


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
        l_shipdate_greaterequal->addChilds({l_shipdate,castLineitemDate2});







        Column *l_discount = new Column("0","l_discount","double");

        DoubleLiteral *zeroPointZeroOne = new DoubleLiteral("0","0.01");

        FunctionCall *add0P01 = new FunctionCall("0","add","double");
        FunctionCall *sub0P01 = new FunctionCall("0","subtract","double");

        add0P01->addChilds({l_discount,zeroPointZeroOne});
        sub0P01->addChilds({l_discount,zeroPointZeroOne});


        FunctionCall *l_discount_greaterEqual =  new FunctionCall("0","greater_than_or_equal_to","bool");
        l_discount_greaterEqual->addChilds({l_discount,sub0P01});
        FunctionCall *l_discount_lessEqual =  new FunctionCall("0","less_than_or_equal_to","bool");
        l_discount_lessEqual->addChilds({l_discount,add0P01});
        FunctionCall *l_discount_and =  new FunctionCall("0","and","bool");
        l_discount_and->addChilds({l_discount_greaterEqual,l_discount_lessEqual});

        Column *l_quantity = new Column("0","l_quantity","int64");
        Int64Literal *l_quantityValue = new Int64Literal("0","24");
        FunctionCall *l_quantity_less =  new FunctionCall("0","less_than","bool");
        l_quantity_less->addChilds({l_quantity,l_quantityValue});




        FunctionCall *and1 =  new FunctionCall("0","and","bool");
        and1->addChilds({l_shipdate_lessthan,l_shipdate_greaterequal});

        FunctionCall *and2 =  new FunctionCall("0","and","bool");
        and2->addChilds({and1,l_discount_and});

        FunctionCall *and3 =  new FunctionCall("0","and","bool");
        and3->addChilds({and2,l_quantity_less});



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

        FilterDescriptor filterDescriptor(fieldsIn,and3);
        FilterNode *filterTime = new FilterNode(UUID::create_uuid(),filterDescriptor);
        return filterTime;
    }



    ProjectNode * createProject()
    {

       // sum(l_extendedprice*l_discount) as revenue



        Column *col_l_discount = new Column("0","l_discount","double");


        Column *col_l_extendedprice = new Column("0","l_extendedprice","double");

        FunctionCall *llPrice = new FunctionCall("0","multiply","double");
        llPrice->addChilds({col_l_discount,col_l_extendedprice});




        ProjectAssignments assignments;

        assignments.addAssignment(FieldDesc("l_orderkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_partkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_suppkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_linenumber","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_quantity","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("llPrice","double"),llPrice);

        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_tax","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_returnflag","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_linestatus","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_shipdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_commitdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_receiptdate","date32"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_shipinstruct","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_shipmode","string"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_comment","string"),FieldDesc::getEmptyDesc(),NULL);


        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;


    }

    PartialAggregationNode *createPartialAgg()
    {

        /*
        sum(l_quantity) as sum_qty, //总的数量
        sum(l_extendedprice) as sum_base_price, //聚集函数操作
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
    */



        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"llPrice",/*outputName=*/"revenue_partial")},
                /*groupByKeys=*/{});


        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {

        /*
        sum(l_quantity) as sum_qty, //总的数量
        sum(l_extendedprice) as sum_base_price, //聚集函数操作
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
    */



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"revenue_partial",/*outputName=*/"revenue")},
                /*groupByKeys=*/{});


        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }



};




#endif //OLVP_QUERY6_HPP
