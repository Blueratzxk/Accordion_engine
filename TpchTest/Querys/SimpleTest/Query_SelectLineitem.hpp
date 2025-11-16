//
// Created by zxk on 11/13/25.
//

#ifndef OLVP_QUERY_SELECTLINEITEM_HPP
#define OLVP_QUERY_SELECTLINEITEM_HPP

#include "../../../Query/RegQuery.h"
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

class Query_SelectLineitem:public RegQuery
{

public:
    Query_SelectLineitem(){

    }
    string getSql()  {return TpchSqls::Q1();}
    PlanNode* getPlanTree()
    {
        TableScanNode *tableScanLineItem = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","lineitem"));

        shared_ptr<PartitioningScheme> schemetableScan = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *tableScanExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemetableScan,tableScanLineItem);


        PlanNode *project = createProject();
        project->addSource(tableScanExchange);



        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());
        output->addSource(project);

        return (PlanNode*)output;
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



};






#endif //OLVP_QUERY_SELECTLINEITEM_HPP
