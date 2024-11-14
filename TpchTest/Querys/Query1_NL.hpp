//
// Created by zxk on 4/15/24.
//

#ifndef OLVP_QUERY1_NL_HPP
#define OLVP_QUERY1_NL_HPP
#include "../../Query/RegQuery.h"
/*
select
        l_returnflag, //返回标志
l_linestatus,
        sum(l_quantity) as sum_qty, //总的数量
sum(l_extendedprice) as sum_base_price, //聚集函数操作
sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order //每个分组所包含的行数
from
        lineitem
where
        l_shipdate <= date'1998-12-01' - interval '90' day //时间段是随机生成的
group by //分组操作
l_returnflag,
        l_linestatus
order by //排序操作
l_returnflag,
l_linestatus;
*/


//the row we need return,l_returnflag,l_linestatus,l_quantity,l_extendedprice,func(l_extendedprice,l_discount)=>disc_price
//func(l_extendedprice,l_discount,l_tax)=>charge,l_discount

class Query1_NL:public RegQuery
{

public:
    Query1_NL(){

    }
    string getSql()  {return TpchSqls::Q1();}
    PlanNode* getPlanTree()
    {
        TableScanNode *tableScanLineItem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));

        shared_ptr<PartitioningScheme> schemetableScan = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *tableScanExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemetableScan,tableScanLineItem);

     //   LocalExchangeNode *tableScanLocalExchange = new LocalExchangeNode("tableScanLocalExchange");
     //   tableScanLocalExchange->addSource(tableScanExchange);

        PlanNode *filterTime = createFilterNode();
        filterTime->addSource(tableScanExchange);

        PlanNode *project = createProject();
        project->addSource(filterTime);



        PlanNode *partialAgg = createPartialAgg();
        partialAgg->addSource(project);



        shared_ptr<PartitioningScheme> schemeExchange = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *paggExchange = new ExchangeNode("schemeExchange",ExchangeNode::GATHER,schemetableScan,partialAgg);



        PlanNode *finalAgg = createFinalAgg();
        finalAgg->addSource(paggExchange);

        PlanNode *resultPro = createResultProject();
        resultPro->addSource(finalAgg);


        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(resultPro);
        return (PlanNode*)output;
    }

    FilterNode *createFilterNode()
    {
        Date32Literal *date = new Date32Literal("0","1998-12-01");

        DayTimeIntervalLiteral *interval = new DayTimeIntervalLiteral("0",90);
        FunctionCall *funcDatetimeCompute = new FunctionCall("0","subtract","int32");
        funcDatetimeCompute->addChilds({date,interval});

        FunctionCall *castDate = new FunctionCall("0","castDATE","date32");
        castDate->addChilds({funcDatetimeCompute});

        Column *lshipDate = new Column("0","l_shipdate","date32");

        FunctionCall *funcLess = new FunctionCall("0","less_than_or_equal_to","bool");
        funcLess->addChilds({lshipDate,castDate});


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

        FilterDescriptor filterDescriptor(fieldsIn,funcLess);
        FilterNode *filterTime = new FilterNode(UUID::create_uuid(),filterDescriptor);
        return filterTime;
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




        //l_extendedprice * (1 - l_discount) * (1 + l_tax) ==> charge


        DoubleLiteral *chargeConst1_1 = new DoubleLiteral("0","1");
        Column *charge_col_l_tax = new Column("0","l_tax","double");

        FunctionCall *chargeSum = new FunctionCall("0","add","double");
        chargeSum->addChilds({chargeConst1_1,charge_col_l_tax});


        DoubleLiteral *chargeConst1_2 = new DoubleLiteral("0","1");
        Column *charge_col_l_discount = new Column("0","l_discount","double");
        FunctionCall *charge_sub = new FunctionCall("0","subtract","double");
        charge_sub->addChilds({chargeConst1_2,charge_col_l_discount});


        FunctionCall *charge_mul_1 = new FunctionCall("0","multiply","double");
        charge_mul_1->addChilds({chargeSum,charge_sub});

        FunctionCall *charge = new FunctionCall("0","multiply","double");
        charge->addChilds({charge_mul_1,col_l_extendedprice});




        ProjectAssignments assignments;

        assignments.addAssignment(FieldDesc("l_orderkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_partkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_suppkey","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_linenumber","int64"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_quantity","int64"),FieldDesc("l_quantity","int64"),NULL);
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("disc_price","double"),disc_price);
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("charge","double"),charge);
        assignments.addAssignment(FieldDesc("l_extendedprice","double"),FieldDesc("l_extendedprice","double"),NULL);
        assignments.addAssignment(FieldDesc("l_discount","double"),FieldDesc("l_discount","double"),NULL);
        assignments.addAssignment(FieldDesc("l_tax","double"),FieldDesc::getEmptyDesc(),NULL);
        assignments.addAssignment(FieldDesc("l_returnflag","string"),FieldDesc("l_returnflag","string"),NULL);
        assignments.addAssignment(FieldDesc("l_linestatus","string"),FieldDesc("l_linestatus","string"),NULL);
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
        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"l_quantity",/*outputName=*/"partial_sum_qty"),
                                        AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"l_extendedprice",/*outputName=*/"partial_sum_base_price"),
                                        AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"disc_price",/*outputName=*/"partial_sum_disc_price"),
                                        AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"charge",/*outputName=*/"partial_sum_charge"),
                                        AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"l_discount",/*outputName=*/"partial_sum_l_discount"),
                                        AggregateDesc(/*functionName=*/"hash_count_all",/*inputKey=*/"",/*outputName=*/"partial_count_order")
                                       },
                /*groupByKeys=*/{"l_returnflag","l_linestatus"});

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



        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_sum_qty",/*outputName=*/"sum_qty"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_sum_base_price",/*outputName=*/"sum_base_price"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_sum_disc_price",/*outputName=*/"sum_disc_price"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_sum_charge",/*outputName=*/"sum_charge"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_sum_l_discount",/*outputName=*/"sum_l_discount"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"partial_count_order",/*outputName=*/"count_order")
                                     },
                /*groupByKeys=*/{"l_returnflag","l_linestatus"});

        //    AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"l_quantity",/*outputName=*/"sum_qty"),
        //                                  AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"l_extendedprice",/*outputName=*/"sum_base_price"),
        //                                 AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"disc_price",/*outputName=*/"sum_disc_price"),
        //                                 AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"charge",/*outputName=*/"sum_charge"),
        //                                 AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"l_discount",/*outputName=*/"sum_l_discount"),
        //                                 AggregateDesc(/*functionName=*/"hash_count_all",/*inputKey=*/"",/*outputName=*/"count_order")
        //                                  },
        //                                 /*groupByKeys=*/{"l_returnflag","l_linestatus"});


        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }
    ProjectNode *createResultProject()
    {

        /*
        l_returnflag, //返回标志
                l_linestatus,
                sum(l_quantity) as sum_qty, //总的数量
        sum(l_extendedprice) as sum_base_price, //聚集函数操作
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order //每个分组所包含的行数
        */

        //l_extendedprice * (1 - l_discount) ==> disc_price

        Column *count_order = new Column("0","count_order","int64");
        FunctionCall *cast_countOrder_float8 = new FunctionCall("0","castFLOAT8","double");
        cast_countOrder_float8->addChilds({count_order});



        Column *sum_qty = new Column("0","sum_qty","int64");
        FunctionCall *cast_sumQty_float8 = new FunctionCall("0","castFLOAT8","double");
        cast_sumQty_float8->addChilds({sum_qty});

        FunctionCall *sumQty_div_countOrder = new FunctionCall("0","divide","double");
        sumQty_div_countOrder->addChilds({cast_sumQty_float8,cast_countOrder_float8});





        Column *sum_base_price = new Column("0","sum_base_price","double");


        FunctionCall *sumBasePrice_div_countOrder = new FunctionCall("0","divide","double");
        sumBasePrice_div_countOrder->addChilds({sum_base_price,cast_countOrder_float8});


        Column *sumLdiscount = new Column("0","sum_l_discount","double");
        FunctionCall *sumLdiscount_div_countOrder = new FunctionCall("0","divide","double");
        sumLdiscount_div_countOrder->addChilds({sumLdiscount,cast_countOrder_float8});



        ProjectAssignments assignments;

        assignments.addAssignment(FieldDesc("l_returnflag","string"),FieldDesc("l_returnflag","string"),NULL);
        assignments.addAssignment(FieldDesc("l_linestatus","string"),FieldDesc("l_linestatus","string"),NULL);

        assignments.addAssignment(FieldDesc("sum_qty","int64"),FieldDesc("sum_qty","int64"),NULL);
        assignments.addAssignment(FieldDesc("sum_qty","int64"),FieldDesc("avg_qty","double"),sumQty_div_countOrder);

        assignments.addAssignment(FieldDesc("sum_base_price","double"),FieldDesc("sum_base_price","double"),NULL);
        assignments.addAssignment(FieldDesc("sum_base_price","double"),FieldDesc("avg_price","double"),sumBasePrice_div_countOrder);

        assignments.addAssignment(FieldDesc("sum_disc_price","double"),FieldDesc("sum_disc_price","double"),NULL);

        assignments.addAssignment(FieldDesc("sum_charge","double"),FieldDesc("sum_charge","double"),NULL);

        assignments.addAssignment(FieldDesc("sum_l_discount","double"),FieldDesc("sum_l_discount","double"),NULL);
        assignments.addAssignment(FieldDesc("sum_l_discount","double"),FieldDesc("avg_disc","double"),sumLdiscount_div_countOrder);

        assignments.addAssignment(FieldDesc("count_order","int64"),FieldDesc::getEmptyDesc(),NULL);





        ProjectNode *projectNode = new ProjectNode(UUID::create_uuid(),assignments);
        return projectNode;



    }




};



#endif //OLVP_QUERY1_NL_HPP
