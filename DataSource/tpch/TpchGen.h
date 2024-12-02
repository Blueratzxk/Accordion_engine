/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <iostream>



#include "arrow/io/api.h"
#include "arrow/record_batch.h"
#include "arrow/array.h"
#include "arrow/result.h"


#include "../Utils/ArrowArrayBuilder.hpp"
#include "../Utils/ArrowDicts.hpp"

//#include "../tpch/gen/dbgen/include/tpch_constants.hpp"

#include <vector>
#include <string>
#include <unordered_map>
#include "spdlog/spdlog.h"


class RowType {
    std::vector<std::string> names;
    std::vector<std::string>types;
public:
    RowType(
            std::vector<std::string> types,
            std::vector<std::string> names)
    {
        this->names = names;
        this->types = types;
    }

    const std::vector<std::string> getNames() const {return this->names;}
    const std::vector<std::string> getTypes() const {return this->types;}


    string findChild(string columnName) const {
        int index = 0;
        for(;index < names.size(); index++)
        {
            if(names[index] == columnName)
                return types[index];
        }
        return types[0];
    }

};



namespace facebook::velox::tpch {

/// This file uses TPC-H DBGEN to generate data encoded using Velox Vectors.
///
/// The basic input for the API is the TPC-H table name (the Table enum), the
/// TPC-H scale factor, the maximum batch size, and the offset. The common usage
/// is to make successive calls to this API advancing the offset parameter,
/// until all records were read. Clients might also assign different slices of
/// the range "[0, getRowCount(Table, scaleFactor)[" to different threads in
/// order to generate datasets in parallel.
///
/// If not enough records are available given a particular scale factor and
/// offset, less than maxRows records might be returned.
///
/// Data is always returned in a RowVector.


static    std::shared_ptr<const RowType> ROW(
            std::vector<std::string> names,
            std::vector<std::string> types) {
        return std::make_shared<RowType>(types, names);
    }


    using RowTypePtr = std::shared_ptr<const RowType>;


enum class Table{
  TBL_PART,
  TBL_SUPPLIER,
  TBL_PARTSUPP,
  TBL_CUSTOMER,
  TBL_ORDERS,
  TBL_LINEITEM,
  TBL_NATION,
  TBL_REGION,
};

static constexpr auto tables = {
    tpch::Table::TBL_PART,
    tpch::Table::TBL_SUPPLIER,
    tpch::Table::TBL_PARTSUPP,
    tpch::Table::TBL_CUSTOMER,
    tpch::Table::TBL_ORDERS,
    tpch::Table::TBL_LINEITEM,
    tpch::Table::TBL_NATION,
    tpch::Table::TBL_REGION};

/// Returns table name as a string.
std::string_view toTableName(Table table);

/// Returns the table enum value given a table name.
Table fromTableName(std::string_view tableName);

/// Returns the row count for a particular TPC-H table given a scale factor, as
/// defined in the spec available at:
///
///  https://www.tpc.org/tpch/
size_t getRowCount(Table table, double scaleFactor);
size_t getRowCount(string table,double scaleFactor);

/// Returns the schema (RowType) for a particular TPC-H table.
RowTypePtr getTableSchema(Table table);

/// Returns the type of a particular table:column pair. Throws if `columnName`
/// does not exist in `table`.
//TypePtr resolveTpchColumn(Table table, const std::string& columnName);

/// Returns a row vector containing at most `maxRows` rows of the "orders"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  o_orderkey: BIGINT
///  o_custkey: BIGINT
///  o_orderstatus: VARCHAR
///  o_totalprice: DOUBLE
///  o_orderdate: DATE
///  o_orderpriority: VARCHAR
///  o_clerk: VARCHAR
///  o_shippriority: INTEGER
///  o_comment: VARCHAR
///
shared_ptr<arrow::RecordBatch> genTpchOrders(
    size_t maxRows = 10000,
    size_t offset = 0,
    double scaleFactor = 1);

/// NOTE: This function's parameters have different semantic from the function
/// above. Dbgen does not provide deterministic random access to lineitem
/// generated rows (like it does for all other tables), because the number of
/// lineitems in an order is chosen at random (between 1 and 7, 4 on average).
//
/// In order to make this function reproducible and deterministic, the
/// parameters (maxRows and offset) refer to orders, not lineitems, and thus the
/// number of lineitem returned rows will be on average 4 * maxOrderRows.
///
/// Returns a row vector containing on average `maxOrderRows * 4` (from
/// `maxOrderRows` to `maxOrderRows * 7`) rows of the "lineitem" table. The
/// offset is controlled based on the orders table, starting at `ordersOffset`,
/// and given the scale factor. The row vector returned has the following
/// schema:
///
///  l_orderkey: BIGINT
///  l_partkey: BIGINT
///  l_suppkey: BIGINT
///  l_linenumber: INTEGER
///  l_quantity: DOUBLE
///  l_extendedprice: DOUBLE
///  l_discount: DOUBLE
///  l_tax: DOUBLE
///  l_returnflag: VARCHAR
///  l_linestatus: VARCHAR
///  l_shipdate: DATE
///  l_commitdate: DATE
///  l_receiptdate: DATE
///  l_shipinstruct: VARCHAR
///  l_shipmode: VARCHAR
///  l_comment: VARCHAR
///
shared_ptr<arrow::RecordBatch>  genTpchLineItem(

    size_t maxOrdersRows = 10000,
    size_t ordersOffset = 0,
    double scaleFactor = 1,
    int *orderOffSetMove = NULL);

/// Returns a row vector containing at most `maxRows` rows of the "part"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  p_partkey: BIGINT
///  p_name: VARCHAR
///  p_mfgr: VARCHAR
///  p_brand: VARCHAR
///  p_type: VARCHAR
///  p_size: INTEGER
///  p_container: VARCHAR
///  p_retailprice: DOUBLE
///  p_comment: VARCHAR
///
shared_ptr<arrow::RecordBatch>  genTpchPart(
    size_t maxRows = 10000,
    size_t offset = 0,
    double scaleFactor = 1);

/// Returns a row vector containing at most `maxRows` rows of the "supplier"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  s_suppkey: BIGINT
///  s_name: VARCHAR
///  s_address: VARCHAR
///  s_nationkey: BIGINT
///  s_phone: VARCHAR
///  s_acctbal: DOUBLE
///  s_comment: VARCHAR
///
shared_ptr<arrow::RecordBatch>  genTpchSupplier(

    size_t maxRows = 10000,
    size_t offset = 0,
    double scaleFactor = 1);

/// Returns a row vector containing at most `maxRows` rows of the "partsupp"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  ps_partkey: BIGINT
///  ps_suppkey: BIGINT
///  ps_availqty: INTEGER
///  ps_supplycost: DOUBLE
///  ps_comment: VARCHAR
///
shared_ptr<arrow::RecordBatch>  genTpchPartSupp(

    size_t maxRows = 10000,
    size_t offset = 0,
    double scaleFactor = 1);

/// Returns a row vector containing at most `maxRows` rows of the "customer"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  c_custkey: BIGINT
///  c_name: VARCHAR
///  c_addressname: VARCHAR
///  c_nationkey: BIGINT
///  c_phone: VARCHAR
///  c_acctbal: DOUBLE
///  c_mktsegment: VARCHAR
///  c_comment: VARCHAR
///
shared_ptr<arrow::RecordBatch>  genTpchCustomer(

    size_t maxRows = 10000,
    size_t offset = 0,
    double scaleFactor = 1);

/// Returns a row vector containing at most `maxRows` rows of the "nation"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  n_nationkey: BIGINT
///  n_name: VARCHAR
///  n_regionkey: BIGINT
///  n_comment: VARCHAR
///
shared_ptr<arrow::RecordBatch>  genTpchNation(

    size_t maxRows = 10000,
    size_t offset = 0,
    double scaleFactor = 1);

/// Returns a row vector containing at most `maxRows` rows of the "region"
/// table, starting at `offset`, and given the scale factor. The row vector
/// returned has the following schema:
///
///  r_regionkey: BIGINT
///  r_name: VARCHAR
///  r_comment: VARCHAR
///
///
shared_ptr<arrow::RecordBatch>  genTpchRegion(

    size_t maxRows = 10000,
    size_t offset = 0,
    double scaleFactor = 1);

/// Gets the specified TPC-H query number as a string.
//std::string getQuery(int query);

} // namespace facebook::velox::tpch
