//
// Created by zxk on 11/15/23.
//

#ifndef OLVP_REGQUERY_H
#define OLVP_REGQUERY_H

#include "../Frontend/PlanNode/PlanNodeTree.hpp"

class TpchSqls
{
public:
    static string Q1()
    {
        string sql;
        sql.append("select\n");
        sql.append("\tl_returnflag,\n");
        sql.append("\tl_linestatus,\n");
        sql.append("\tsum(l_quantity) as sum_qty,\n");
        sql.append("\tsum(l_extendedprice) as sum_base_price,\n");
        sql.append("\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n");
        sql.append("\tsum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n");
        sql.append("\tavg(l_quantity) as avg_qty,\n");
        sql.append("\tavg(l_extendedprice) as avg_price,\n");
        sql.append("\tavg(l_discount) as avg_disc,\n");
        sql.append("\tcount(*) as count_order\n");
        sql.append("from\n");
        sql.append("\tlineitem\n");
        sql.append("where\n");
        sql.append("\tl_shipdate <= date'1998-12-01' - interval '90' day\n");
        sql.append("group by\n");
        sql.append("\tl_returnflag,\n");
        sql.append("\tl_linestatus\n");
        sql.append("order by\n");
        sql.append("\tl_returnflag,\n");
        sql.append("\tl_linestatus;\n");

        return sql;
    }


    static string Q2()
    {
        string sql;
        sql.append("select\n");
        sql.append("s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment \n");
        sql.append("from\n");
        sql.append("part, supplier, partsupp, nation, region\n");
        sql.append("where\n");
        sql.append("p_partkey = ps_partkey\n");
        sql.append("and s_suppkey = ps_suppkey\n");
        sql.append("and p_size = [SIZE]\n");
        sql.append("and p_type like '%[TYPE]' \n");
        sql.append("and s_nationkey = n_nationkey\n");
        sql.append("and n_regionkey = r_regionkey\n");
        sql.append("and r_name = '[REGION]'\n");
        sql.append("and ps_supplycost = ( \n");
        sql.append("select\n");
        sql.append("min(ps_supplycost)\n");
        sql.append("from\n");
        sql.append("partsupp, supplier, nation, region\n");
        sql.append("where\n");
        sql.append("p_partkey = ps_partkey\n");
        sql.append("and s_suppkey = ps_suppkey\n");
        sql.append("and s_nationkey = n_nationkey\n");
        sql.append("and n_regionkey = r_regionkey\n");
        sql.append("and r_name = '[REGION]'\n");
        sql.append(")\n");
        sql.append("order by\n");
        sql.append("s_acctbal desc,\n");
        sql.append("n_name,\n");
        sql.append("s_name,\n");
        sql.append("p_partkey;\n");
        return sql;
    }

    static string Q3()
    {
        string sql;
        sql.append("select\n");
        sql.append("l_orderkey,\n");
        sql.append("sum(l_extendedprice*(1-l_discount)) as revenue,\n");
        sql.append("o_orderdate,\n");
        sql.append("o_shippriority\n");
        sql.append("from\n");
        sql.append("customer, orders, lineitem\n");
        sql.append("where\n");
        sql.append("c_mktsegment = '[SEGMENT]' \n");
        sql.append("and c_custkey = o_custkey\n");
        sql.append("and l_orderkey = o_orderkey\n");
        sql.append("and o_orderdate < date '[DATE]' \n");
        sql.append("and l_shipdate > date '[DATE]'\n");
        sql.append(" group by\n");
        sql.append("l_orderkey,\n");
        sql.append("o_orderdate,\n");
        sql.append("o_shippriority\n");
        sql.append("order by\n");
        sql.append("revenue desc,\n");
        sql.append("o_orderdate;\n");
        return sql;
    }


    static string Q4()
    {
        string sql;


        sql.append("select\n");
        sql.append("o_orderpriority, \n");
        sql.append("count(*) as order_count\n");
        sql.append("from orders\n");
        sql.append("where\n");
        sql.append("o_orderdate >= date '[DATE]'\n");
        sql.append("and o_orderdate < date '[DATE]' + interval '3' month\n");
        sql.append("and exists ( \n");
        sql.append("select\n");
        sql.append("*\n");
        sql.append("from\n");
        sql.append("lineitem\n");
        sql.append("where\n");
        sql.append("l_orderkey = o_orderkey\n");
        sql.append("and l_commitdate < l_receiptdate\n");
        sql.append(")\n");
        sql.append("group by \n");
        sql.append("o_orderpriority\n");
        sql.append("order by \n");
        sql.append("o_orderpriority;\n");


        return sql;
    }

    static string Q5()
    {
        string sql;
        sql.append(" select\n");
        sql.append("n_name,\n");
        sql.append(" sum(l_extendedprice * (1 - l_discount)) as revenue \n");
        sql.append("from\n");
        sql.append("customer,orders,lineitem,supplier,nation,region \n");
        sql.append("where\n");
        sql.append("c_custkey = o_custkey\n");
        sql.append("and l_orderkey = o_orderkey\n");
        sql.append("and l_suppkey = s_suppkey\n");
        sql.append("and c_nationkey = s_nationkey\n");
        sql.append("and s_nationkey = n_nationkey\n");
        sql.append("and n_regionkey = r_regionkey\n");
        sql.append("and r_name = '[REGION]'\n");
        sql.append("and o_orderdate >= date '[DATE]' \n");
        sql.append("and o_orderdate < date '[DATE]' + interval '1' year\n");
        sql.append("group by \n");
        sql.append("n_name\n");
        sql.append("order by \n");
        sql.append("revenue desc;\n");
        return sql;
    }

    static string Q6()
    {
        string sql;
        sql.append("select\n");
        sql.append("sum(l_extendedprice*l_discount) as revenue\n");
        sql.append("from\n");
        sql.append("lineitem\n");
        sql.append("where\n");
        sql.append("l_shipdate >= date '[DATE]' \n");
        sql.append("and l_shipdate < date '[DATE]' + interval '1' year\n");
        sql.append("and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01 \n");
        sql.append("and l_quantity < [QUANTITY]; \n");
        return sql;
    }
    static string Q7()
    {
        string sql;
        sql.append("select\n");
        sql.append("supp_nation, \n");
        sql.append("cust_nation, \n");
        sql.append(" l_year, sum(volume) as revenue \n");
        sql.append("from (\n");
        sql.append("select\n");
        sql.append("n1.n_name as supp_nation,\n");
        sql.append("n2.n_name as cust_nation,\n");
        sql.append("extract(year from l_shipdate) as l_year,\n");
        sql.append("l_extendedprice * (1 - l_discount) as volume\n");
        sql.append("from\n");
        sql.append("supplier,lineitem,orders,customer,nation n1,nation n2 \n");
        sql.append("where\n");
        sql.append("s_suppkey = l_suppkey\n");
        sql.append("and o_orderkey = l_orderkey\n");
        sql.append("and c_custkey = o_custkey\n");
        sql.append("and s_nationkey = n1.n_nationkey\n");
        sql.append("and c_nationkey = n2.n_nationkey\n");
        sql.append("and (\n");
        sql.append("(n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]')\n");
        sql.append("or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]')\n");
        sql.append(")\n");
        sql.append("and l_shipdate between date '1995-01-01' and date '1996-12-31'\n");
        sql.append(") as shipping\n");
        sql.append("group by\n");
        sql.append("supp_nation,\n");
        sql.append("cust_nation,\n");
        sql.append("l_year\n");
        sql.append("order by\n");
        sql.append("supp_nation,\n");
        sql.append("cust_nation,\n");
        sql.append("l_year;\n");
        return sql;
    }

    static string Q8()
    {
        string sql;
        sql.append("select\n");
        sql.append("o_year, \n");
        sql.append("sum(case\n");
        sql.append("when nation = '[NATION]'\n");
        sql.append("then volume\n");
        sql.append("else 0\n");
        sql.append("end) / sum(volume) as mkt_share\n");
        sql.append("from \n");
        sql.append("(select\n");
        sql.append("extract(year from o_orderdate) as o_year, \n");
        sql.append("l_extendedprice * (1-l_discount) as volume, \n");
        sql.append("n2.n_name as nation\n");
        sql.append("from\n");
        sql.append("part,supplier,lineitem,orders,customer,nation n1,nation n2,region \n");
        sql.append("where\n");
        sql.append("p_partkey = l_partkey\n");
        sql.append("and s_suppkey = l_suppkey\n");
        sql.append("and l_orderkey = o_orderkey\n");
        sql.append("and o_custkey = c_custkey\n");
        sql.append("and c_nationkey = n1.n_nationkey\n");
        sql.append("and n1.n_regionkey = r_regionkey\n");
        sql.append("and r_name = '[REGION]'\n");
        sql.append("and s_nationkey = n2.n_nationkey\n");
        sql.append("and o_orderdate between date '1995-01-01' and date '1996-12-31' \n");
        sql.append(" and p_type = '[TYPE]'\n");
        sql.append(") as all_nations\n");
        sql.append("group by \n");
        sql.append("o_year\n");
        sql.append("order by\n");
        sql.append("o_year;\n");
        return sql;
    }

    static string Q9()
    {
        string sql;
        sql.append("select\n");
        sql.append("nation,\n");
        sql.append("o_year,\n");
        sql.append("sum(amount) as sum_profit \n");
        sql.append("from\n");
        sql.append("(select\n");
        sql.append("n_name as nation, \n");
        sql.append("extract(year from o_orderdate) as o_year, \n");
        sql.append("l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n");
        sql.append("from\n");
        sql.append("part,supplier,lineitem,partsupp,orders,nation \n");
        sql.append("where\n");
        sql.append("s_suppkey = l_suppkey\n");
        sql.append("and ps_suppkey = l_suppkey\n");
        sql.append("and ps_partkey = l_partkey\n");
        sql.append("and p_partkey = l_partkey\n");
        sql.append("and o_orderkey = l_orderkey\n");
        sql.append("and s_nationkey = n_nationkey\n");
        sql.append("and p_name like '%[COLOR]%'\n");
        sql.append(") as profit\n");
        sql.append("group by \n");
        sql.append("nation,\n");
        sql.append("o_year\n");
        sql.append("order by\n");
        sql.append("nation,\n");
        sql.append("o_year desc;\n");
        return sql;
    }

    static string Q10()
    {
        string sql;
        sql.append("select\n");
        sql.append("c_custkey, c_name, \n");
        sql.append("sum(l_extendedprice * (1 - l_discount)) as revenue,\n");
        sql.append("c_acctbal,\n");
        sql.append("n_name, c_address, c_phone, c_comment \n");
        sql.append("from\n");
        sql.append("customer, orders, lineitem, nation\n");
        sql.append("where\n");
        sql.append("c_custkey = o_custkey\n");
        sql.append("and l_orderkey = o_orderkey\n");
        sql.append("and o_orderdate >= date '[DATE]'\n");
        sql.append("and o_orderdate < date '[DATE]' + interval '3' month\n");
        sql.append("and l_returnflag = 'R'\n");
        sql.append("and c_nationkey = n_nationkey\n");
        sql.append(" group by\n");
        sql.append("c_custkey,\n");
        sql.append("c_name,\n");
        sql.append("c_acctbal,\n");
        sql.append("c_phone,\n");
        sql.append("n_name,\n");
        sql.append("c_address,\n");
        sql.append("c_comment\n");
        sql.append("order by\n");
        sql.append("revenue desc;\n");
        return sql;
    }

    static string Q11()
    {
        string sql;
        sql.append("select\n");
        sql.append("ps_partkey,\n");
        sql.append("sum(ps_supplycost * ps_availqty) as value \n");
        sql.append("from\n");
        sql.append("partsupp, supplier, nation\n");
        sql.append("where\n");
        sql.append("ps_suppkey = s_suppkey\n");
        sql.append("and s_nationkey = n_nationkey\n");
        sql.append("and n_name = '[NATION]'\n");
        sql.append("group by\n");
        sql.append("ps_partkey having\n");
        sql.append("sum(ps_supplycost * ps_availqty) > ( \n");
        sql.append("select\n");
        sql.append("sum(ps_supplycost * ps_availqty) * [FRACTION] \n");
        sql.append("from\n");
        sql.append("partsupp, supplier, nation\n");
        sql.append("where \n");
        sql.append("ps_suppkey = s_suppkey\n");
        sql.append("and s_nationkey = n_nationkey\n");
        sql.append("and n_name = '[NATION]'\n");
        sql.append(")\n");
        sql.append(" order by \n");
        sql.append("value desc;\n");
        return sql;
    }

    static string Q12()
    {
        string sql;
        sql.append("select\n");
        sql.append("l_shipmode,\n");
        sql.append("sum(case \n");
        sql.append("when o_orderpriority ='1-URGENT' \n");
        sql.append("or o_orderpriority ='2-HIGH'\n");
        sql.append("then 1\n");
        sql.append("else 0\n");
        sql.append("end) as high_line_count,\n");
        sql.append("sum(case\n");
        sql.append("when o_orderpriority <> '1-URGENT'\n");
        sql.append("and o_orderpriority <> '2-HIGH'\n");
        sql.append("then 1\n");
        sql.append("else 0\n");
        sql.append("end) as low_line_count\n");
        sql.append("from\n");
        sql.append("orders,lineitem\n");
        sql.append("where\n");
        sql.append("o_orderkey = l_orderkey\n");
        sql.append("and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]') \n");
        sql.append("and l_commitdate < l_receiptdate\n");
        sql.append("and l_shipdate < l_commitdate\n");
        sql.append("and l_receiptdate >= date '[DATE]' \n");
        sql.append("and l_receiptdate < date '[DATE]' + interval '1' year //1ÄêÖ®ÄÚ\n");
        sql.append("group by \n");
        sql.append("l_shipmode\n");
        sql.append("order by\n");
        sql.append("l_shipmode;\n");
        return sql;
    }
    static string Q2J()
    {
        string sql;
        sql.append("select count(*) from lineitem,orders where lineitem.l_orderkey = orders.o_orderkey;");

        return sql;
    }
    static string Q2J_SmallOrders()
    {
        string sql;
        sql.append("select count(*) from lineitem,orders where lineitem.l_orderkey = orders.o_orderkey and orders.orderkey < 30;");

        return sql;
    }
    static string Q2J2()
    {
        string sql;
        sql.append("select count(*) from lineitem,supplier where lineitem.l_suppkey = supplier.s_suppkey;");

        return sql;
    }
    static string QSJS()
    {
        string sql;
        sql.append("select count(*) from supplier as s1,supplier as s2 where s1.s_suppkey = s2.s_suppkey;");

        return sql;
    }


};

class RegQuery
{

public:
    virtual PlanNode* getPlanTree() = 0;
    virtual string getSql()  = 0;

};


#endif //OLVP_REGQUERY_H
