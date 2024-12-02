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

#include <iomanip>
#include <sstream>
#include "TpchGen.h"
#include "gen/DBGenIterator.h"
namespace facebook::velox::tpch {
namespace {

using namespace dbgen;




    std::string BIGINT(){return "int64";}
    std::string  VARCHAR(){return "string";}
    std::string  INTEGER(){return "int64";}
    std::string  DOUBLE(){return "double";}
    std::string  DATE(){return "date32";}


    void str_to_tm(char *p_time, struct tm* m_tm)
    {
        if(p_time)
        {
            sscanf(p_time, "%4d%2d%2d-%d:%d:%d", &m_tm->tm_year,&m_tm->tm_mon,&m_tm->tm_mday,&m_tm->tm_hour,&m_tm->tm_min,&m_tm->tm_sec);
            m_tm->tm_mon -= 1;
            m_tm->tm_year -= 1900;
            printf("%4d%2d%2d-%d:%d:%d\n", m_tm->tm_year,m_tm->tm_mon,m_tm->tm_mday,m_tm->tm_hour,m_tm->tm_min,m_tm->tm_sec);
        }
        else
        {
            printf("input time is null\n");
            return ;
        }
    }

/*时间结构转换为时间戳*/
    time_t time_to_stamp(const struct tm* ltm, int utc_diff)
    {
        const int mon_days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
        long tyears,tdays,leap_years,utc_hrs;
        int is_leap;
        int i,ryear;

        //判断闰年
        ryear = ltm->tm_year + 1900;
        is_leap = ((ryear%100!=0 && ryear%4==0) || (ryear%400==0) ) ? 1 : 0;

        tyears = ltm->tm_year-70;  //时间戳从1970年开始算起
        if(ltm->tm_mon < 1 && is_leap==1 )
        {
            leap_years = (tyears + 2) / 4 - 1;  //1970年不是闰年，从1972年开始闰年
            //闰年的月份小于1，需要减去一天
        }
        else
        {
            leap_years = (tyears + 2) / 4 ;
        }

        tdays = 0;
        for(i=0; i<ltm->tm_mon; ++i)
        {
            tdays += mon_days[i];
        }
        tdays += ltm->tm_mday - 1;  //减去今天
        tdays += tyears * 365 + leap_years;
        utc_hrs = ltm->tm_hour - utc_diff;   //如上面解释所说，时间戳转换北京时间需要+8，那么这里反转需要-8

        return (tdays * 86400) + (utc_hrs * 3600) + (ltm->tm_min * 60) + ltm->tm_sec;
    }




    bool getDate32(string dateString,int32_t *out)
    {

      //  *out = 10;
      //  return true;

        //"2017-06-08 09:00:05"
        struct tm tm;

        std::stringstream ss(dateString);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");


        if(ss.fail()){
            *out = -1;
            spdlog::critical("Date string format error! The value is set to -1!");
            return false;
        }


        time_t timeStamp = std::mktime(&tm);
        *out = timeStamp/3600/24;
        return true;
    }

    bool toDate32(string dateString,int32_t *out)
    {

        //  *out = 10;
        //  return true;

        //"2017-06-08 09:00:05"
        struct tm tm;

        std::stringstream ss(dateString);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");


        if(ss.fail()){
            *out = -1;
            spdlog::critical("Date string format error! The value is set to -1!");
            return false;
        }

        str_to_tm(dateString.data(),&tm);
        auto timeStamp = time_to_stamp(&tm,8);
        *out = timeStamp/3600/24;
        return true;
    }

    int toDate(string dateStr)
    {
        int out;
        toDate32(dateStr,&out);
        return out;
    }
    // The cardinality of the LINEITEM table is not a strict multiple of SF since
// the number of lineitems in an order is chosen at random with an average of
// four. This function contains the row count for all authorized scale factors
// (as described by the TPC-H spec), and approximates the remaining.
constexpr size_t getLineItemRowCount(double scaleFactor) {
  auto longScaleFactor = static_cast<long>(scaleFactor);
  switch (longScaleFactor) {
    case 1:
      return 6'001'215;
    case 10:
      return 59'986'052;
    case 30:
      return 179'998'372;
    case 100:
      return 600'037'902;
    case 300:
      return 1'799'989'091;
    case 1'000:
      return 5'999'989'709;
    case 3'000:
      return 18'000'048'306;
    case 10'000:
      return 59'999'994'267;
    case 30'000:
      return 179'999'978'268;
    case 100'000:
      return 599'999'969'200;
    default:
      break;
  }
  return 6'000'000 * scaleFactor;
}

size_t getVectorSize(size_t rowCount, size_t maxRows, size_t offset) {
  if (offset >= rowCount) {
    return 0;
  }
  return std::min(rowCount - offset, maxRows);
}

/*
std::vector<VectorPtr> allocateVectors(
    const RowTypePtr& type,
    size_t vectorSize,
    memory::MemoryPool* pool) {
  std::vector<VectorPtr> vectors;
  vectors.reserve(type->size());

  for (const auto& childType : type->children()) {
    vectors.emplace_back(BaseVector::create(childType, vectorSize, pool));
  }
  return vectors;
}
 */

std::vector<std::shared_ptr<arrow::ArrayBuilder>> allocateVectors(
        const RowTypePtr& type) {
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> arrayBuilders;

    for (const auto& childType : type->getTypes()) {

        auto type = Typer::getType(childType);
        auto builder = ArrowArrayBuilder(type).getBuilder();
        arrayBuilders.push_back(builder);
    }


    return arrayBuilders;
}



double decimalToDouble(int64_t value) {
  return (double)value * 0.01;
}

//int32_t toDate(std::string_view stringDate) {
//  return DATE()->toDays(stringDate);
//}

} // namespace

std::string_view toTableName(Table table) {
  switch (table) {
    case Table::TBL_PART:
      return "part";
    case Table::TBL_SUPPLIER:
      return "supplier";
    case Table::TBL_PARTSUPP:
      return "partsupp";
    case Table::TBL_CUSTOMER:
      return "customer";
    case Table::TBL_ORDERS:
      return "orders";
    case Table::TBL_LINEITEM:
      return "lineitem";
    case Table::TBL_NATION:
      return "nation";
    case Table::TBL_REGION:
      return "region";
  }
  return ""; // make gcc happy.
}

Table fromTableName(std::string tableName) {
  static std::unordered_map<std::string, Table> map{
      {"part", Table::TBL_PART},
      {"supplier", Table::TBL_SUPPLIER},
      {"partsupp", Table::TBL_PARTSUPP},
      {"customer", Table::TBL_CUSTOMER},
      {"orders", Table::TBL_ORDERS},
      {"lineitem", Table::TBL_LINEITEM},
      {"nation", Table::TBL_NATION},
      {"region", Table::TBL_REGION},
  };

  auto it = map.find(tableName);
  if (it != map.end()) {
    return it->second;
  }
  spdlog::error("Invalid TPC-H table name: '{}'"+tableName);
}

size_t getRowCount(string table,double scaleFactor)
{
    if(table == "PART" || table == "part")
        return 200'000 * scaleFactor;
    if(table == "SUPPLIER" || table == "supplier")
        return 10'000 * scaleFactor;
    if(table == "PARTSUPP" || table == "partsupp")
        return 800'000 * scaleFactor;
    if(table == "CUSTOMER" || table == "customer")
        return 150'000 * scaleFactor;
    if(table == "ORDERS" || table == "orders")
        return 1'500'000 * scaleFactor;
    if(table == "NATION" || table == "nation")
        return 25;
    if(table == "REGION" || table == "region")
        return 5;
    if(table == "LINEITEM" || table == "lineitem")
        return getLineItemRowCount(scaleFactor);

}

size_t getRowCount(Table table, double scaleFactor) {

  switch (table) {
    case Table::TBL_PART:
      return 200'000 * scaleFactor;
    case Table::TBL_SUPPLIER:
      return 10'000 * scaleFactor;
    case Table::TBL_PARTSUPP:
      return 800'000 * scaleFactor;
    case Table::TBL_CUSTOMER:
      return 150'000 * scaleFactor;
    case Table::TBL_ORDERS:
      return 1'500'000 * scaleFactor;
    case Table::TBL_NATION:
      return 25;
    case Table::TBL_REGION:
      return 5;
    case Table::TBL_LINEITEM:
      return getLineItemRowCount(scaleFactor);
  }
  return 0; // make gcc happy.
}

RowTypePtr getTableSchema(Table table) {
  switch (table) {
    case Table::TBL_PART: {
      static RowTypePtr type = ROW(
          {
              "p_partkey",
              "p_name",
              "p_mfgr",
              "p_brand",
              "p_type",
              "p_size",
              "p_container",
              "p_retailprice",
              "p_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_SUPPLIER: {
      static RowTypePtr type = ROW(
          {
              "s_suppkey",
              "s_name",
              "s_address",
              "s_nationkey",
              "s_phone",
              "s_acctbal",
              "s_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_PARTSUPP: {
      static RowTypePtr type = ROW(
          {
              "ps_partkey",
              "ps_suppkey",
              "ps_availqty",
              "ps_supplycost",
              "ps_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              INTEGER(),
              DOUBLE(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_CUSTOMER: {
      static RowTypePtr type = ROW(
          {
              "c_custkey",
              "c_name",
              "c_address",
              "c_nationkey",
              "c_phone",
              "c_acctbal",
              "c_mktsegment",
              "c_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_ORDERS: {
      static RowTypePtr type = ROW(
          {
              "o_orderkey",
              "o_custkey",
              "o_orderstatus",
              "o_totalprice",
              "o_orderdate",
              "o_orderpriority",
              "o_clerk",
              "o_shippriority",
              "o_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              VARCHAR(),
              DOUBLE(),
              DATE(),
              VARCHAR(),
              VARCHAR(),
              INTEGER(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_LINEITEM: {
      static RowTypePtr type = ROW(
          {
              "l_orderkey",
              "l_partkey",
              "l_suppkey",
              "l_linenumber",
              "l_quantity",
              "l_extendedprice",
              "l_discount",
              "l_tax",
              "l_returnflag",
              "l_linestatus",
              "l_shipdate",
              "l_commitdate",
              "l_receiptdate",
              "l_shipinstruct",
              "l_shipmode",
              "l_comment",
          },
          {
              BIGINT(),
              BIGINT(),
              BIGINT(),
              INTEGER(),
              BIGINT(),
              DOUBLE(),
              DOUBLE(),
              DOUBLE(),
              VARCHAR(),
              VARCHAR(),
              DATE(),
              DATE(),
              DATE(),
              VARCHAR(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }

    case Table::TBL_NATION: {
      static RowTypePtr type = ROW(
          {
              "n_nationkey",
              "n_name",
              "n_regionkey",
              "n_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              BIGINT(),
              VARCHAR(),
          });
      return type;
    }
    case Table::TBL_REGION: {
      static RowTypePtr type = ROW(
          {
              "r_regionkey",
              "r_name",
              "r_comment",
          },
          {
              BIGINT(),
              VARCHAR(),
              VARCHAR(),
          });
      return type;
    }
  }
  return nullptr; // make gcc happy.
}

//std::string resolveTpchColumn(Table table, const std::string& columnName) {
//  return getTableSchema(table)->findChild(columnName);
//}

shared_ptr<arrow::RecordBatch> getRowVector(RowTypePtr rowTypePtr,std::vector<std::shared_ptr<arrow::ArrayBuilder>> childrenRow)
{
    vector<shared_ptr<arrow::ArrayData>> columns;
    for(auto row: childrenRow)
    {
        shared_ptr<arrow::ArrayData> data = row->Finish().ValueOrDie()->data();
        columns.push_back(data);
    }

    vector<string> names = rowTypePtr->getNames();
    vector<string> types = rowTypePtr->getTypes();

    arrow::FieldVector fieldVector;
    for(int i = 0 ; i < names.size() ; i++)
        fieldVector.push_back(make_shared<arrow::Field>(names[i],Typer::getType(types[i])));
    shared_ptr<arrow::Schema> schema = make_shared<arrow::Schema>(fieldVector);

    auto rowVector = arrow::RecordBatch::Make(schema,columns[0]->length,columns);

    if(columns[0]->length == 0)
        return NULL;

    return rowVector;
}

shared_ptr<arrow::RecordBatch> genTpchOrders(

    size_t maxRows,
    size_t offset,
    double scaleFactor) {
  // Create schema and allocate vectors.
  auto ordersRowType = getTableSchema(Table::TBL_ORDERS);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_ORDERS, scaleFactor), maxRows, offset);
  auto childrenRow = allocateVectors(ordersRowType);


    auto orderKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto custKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[1]);
    auto orderStatusVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[2]);
    auto totalPriceVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[3]);
    auto orderDateVector = dynamic_pointer_cast<arrow::Date32Builder>(childrenRow[4]);
    auto orderPriorityVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[5]);
    auto clerkVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[6]);
    auto shipPriorityVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[7]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[8]);

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initOrder(offset);
  order_t order;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.


  arrow::Status status;
  for (size_t i = 0; i < vectorSize; ++i) {
      dbgenIt.genOrder(i + offset + 1, order);

      status = orderKeyVector->Append(order.okey);
      status = custKeyVector->Append(order.custkey);
      status = orderStatusVector->Append(string(&order.orderstatus, 1));
      status = totalPriceVector->Append(decimalToDouble(order.totalprice));
      status = orderDateVector->Append(toDate(order.odate));
      status = orderPriorityVector->Append(string(order.opriority, strlen(order.opriority)));
      status = clerkVector->Append(string(order.clerk, strlen(order.clerk)));
      status = shipPriorityVector->Append(order.spriority);
      status = commentVector->Append(string(order.comment, order.clen));
  }


  return getRowVector(ordersRowType,childrenRow);
}

shared_ptr<arrow::RecordBatch> genTpchLineItem(

    size_t maxOrderRows,
    size_t ordersOffset,
    double scaleFactor,
    int *ordersOffSetMove) {
  // We control the buffer size based on the orders table, then allocate the
  // underlying buffer using the worst case (orderVectorSize * 7).
  size_t orderVectorSize = getVectorSize(
      getRowCount(Table::TBL_ORDERS, scaleFactor), maxOrderRows, ordersOffset);
  size_t lineItemUpperBound = orderVectorSize * 7;

  // Create schema and allocate vectors.
  auto lineItemRowType = getTableSchema(Table::TBL_LINEITEM);
  auto childrenRow = allocateVectors(lineItemRowType);

    auto orderKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto partKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[1]);
    auto suppKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[2]);
    auto lineNumberVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[3]);

    auto quantityVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[4]);
    auto extendedPriceVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[5]);
    auto discountVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[6]);
    auto taxVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[7]);

    auto returnFlagVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[8]);
    auto lineStatusVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[9]);
    auto shipDateVector = dynamic_pointer_cast<arrow::Date32Builder>(childrenRow[10]);
    auto commitDateVector = dynamic_pointer_cast<arrow::Date32Builder>(childrenRow[11]);
    auto receiptDateVector = dynamic_pointer_cast<arrow::Date32Builder>(childrenRow[12]);
    auto shipInstructVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[13]);
    auto shipModeVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[14]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[15]);

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initOrder(ordersOffset);
  order_t order;

  // Dbgen can't generate lineItem one row at a time; instead, it generates
  // orders with a random number of lineitems associated. So we treat offset
  // and maxRows as being in terms of orders (to make it deterministic), and
  // return a RowVector with a variable number of rows.
  size_t lineItemCount = 0;

  arrow::Status status;
  (*ordersOffSetMove) = 0;
  for (size_t i = 0; i < orderVectorSize; ++i) {
    dbgenIt.genOrder(i + ordersOffset + 1, order);
    (*ordersOffSetMove)++;
    for (size_t l = 0; l < order.lines; ++l) {
        const auto &line = order.l[l];
        status = orderKeyVector->Append(line.okey);
        status = partKeyVector->Append(line.partkey);
        status = suppKeyVector->Append(line.suppkey);

        status = lineNumberVector->Append(line.lcnt);

        status = quantityVector->Append(line.quantity);
        status = extendedPriceVector->Append(decimalToDouble(line.eprice));
        status = discountVector->Append(decimalToDouble(line.discount));
        status = taxVector->Append(decimalToDouble(line.tax));

        status = returnFlagVector->Append(string(line.rflag, 1));
        status = lineStatusVector->Append(string(line.lstatus, 1));

        status = shipDateVector->Append(toDate(line.sdate));
        status = commitDateVector->Append(toDate(line.cdate));
        status = receiptDateVector->Append(toDate(line.rdate));

        status = shipInstructVector->Append(string(line.shipinstruct, strlen(line.shipinstruct)));
        status = shipModeVector->Append(string(line.shipmode, strlen(line.shipmode)));
        status = commentVector->Append(string(line.comment, strlen(line.comment)));
    }
    lineItemCount += order.lines;
  }

  // Resize to shrink the buffers - since we allocated based on the upper bound.
 // for (auto& child : childrenRow) {
 //   child->resize(lineItemCount);
 // }
  //return std::make_shared<RowVector>(
 //     pool,
 //    lineItemRowType,
 //    BufferPtr(nullptr),
  //    lineItemCount,
  //    std::move(children));
    return getRowVector(lineItemRowType,childrenRow);

}

shared_ptr<arrow::RecordBatch> genTpchPart(

    size_t maxRows,
    size_t offset,
    double scaleFactor) {
  // Create schema and allocate vectors.
  auto partRowType = getTableSchema(Table::TBL_PART);
  size_t vectorSize =
      getVectorSize(getRowCount(Table::TBL_PART, scaleFactor), maxRows, offset);
  auto childrenRow = allocateVectors(partRowType);

    auto partKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto nameVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[1]);
    auto mfgrVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[2]);
    auto brandVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[3]);
    auto typeVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[4]);
    auto sizeVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[5]);
    auto containerVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[6]);
    auto retailPriceVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[7]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[8]);

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initPart(offset);
  part_t part;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  arrow::Status status;
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genPart(i + offset + 1, part);

      status = partKeyVector->Append(part.partkey);
      status =  nameVector->Append(string(part.name, strlen(part.name)));
      status = mfgrVector->Append(string(part.mfgr, strlen(part.mfgr)));
      status = brandVector->Append(string(part.brand, strlen(part.brand)));
      status = typeVector->Append(string(part.type, part.tlen));
      status = sizeVector->Append(part.size);
      status = containerVector->Append(string(part.container, strlen(part.container)));
      status = retailPriceVector->Append(decimalToDouble(part.retailprice));
      status = commentVector->Append(string(part.comment, part.clen));
  }
 // return std::make_shared<RowVector>(
 //     pool, partRowType, BufferPtr(nullptr), vectorSize, std::move(children));


    return getRowVector(partRowType,childrenRow);
}

shared_ptr<arrow::RecordBatch> genTpchSupplier(

    size_t maxRows,
    size_t offset,
    double scaleFactor) {
  // Create schema and allocate vectors.
  auto supplierRowType = getTableSchema(Table::TBL_SUPPLIER);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_SUPPLIER, scaleFactor), maxRows, offset);
  auto childrenRow = allocateVectors(supplierRowType);


    auto suppKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto nameVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[1]);
    auto addressVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[2]);
    auto nationKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[3]);
    auto phoneVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[4]);
    auto acctbalVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[5]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[6]);

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initSupplier(offset);
  supplier_t supp;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.

  arrow::Status status;
  for (size_t i = 0; i < vectorSize; ++i) {
    dbgenIt.genSupplier(i + offset + 1, supp);

      status = suppKeyVector->Append(supp.suppkey);
      status =  nameVector->Append(string(supp.name, strlen(supp.name)));
      status =  addressVector->Append(string(supp.address, supp.alen));
      status = nationKeyVector->Append(supp.nation_code);
      status = phoneVector->Append(string(supp.phone, strlen(supp.phone)));
      status = acctbalVector->Append(decimalToDouble(supp.acctbal));
      status = commentVector->Append(string(supp.comment, supp.clen));
  }
  /*
  return std::make_shared<RowVector>(
      pool,
      supplierRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));*/

    return getRowVector(supplierRowType,childrenRow);
}


shared_ptr<arrow::RecordBatch> genTpchPartSupp(

    size_t maxRows,
    size_t offset,
    double scaleFactor) {
  // Create schema and allocate vectors.
  auto partSuppRowType = getTableSchema(Table::TBL_PARTSUPP);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_PARTSUPP, scaleFactor), maxRows, offset);
  auto childrenRow = allocateVectors(partSuppRowType);


    auto partKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto suppKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[1]);
    auto availQtyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[2]);
    auto supplyCostVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[3]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[4]);


  DBGenIterator dbgenIt(scaleFactor);
  part_t part;

  // The iteration logic is a bit more complicated as partsupp records are
  // generated using mk_part(), which returns a vector of 4 (SUPP_PER_PART)
  // partsupp record at a time. So we need to align the user's requested window
  // (maxRows, offset), with the 4-at-a-time record window provided by DBGEN.
  size_t partIdx = offset / SUPP_PER_PART;
  size_t partSuppIdx = offset % SUPP_PER_PART;
  size_t partSuppCount = 0;

  dbgenIt.initPart(partIdx);

  arrow::Status status;

  do {
    dbgenIt.genPart(partIdx + 1, part);

    while ((partSuppIdx < SUPP_PER_PART) && (partSuppCount < vectorSize)) {
        const auto &partSupp = part.s[partSuppIdx];

        status = partKeyVector->Append(partSupp.partkey);
        status = suppKeyVector->Append(partSupp.suppkey);
        status = availQtyVector->Append(partSupp.qty);
        status = supplyCostVector->Append(decimalToDouble(partSupp.scost));
        status = commentVector->Append(string(partSupp.comment, partSupp.clen));

        ++partSuppIdx;
        ++partSuppCount;
    }
    partSuppIdx = 0;
    ++partIdx;

  } while (partSuppCount < vectorSize);


 /* return std::make_shared<RowVector>(
      pool,
      partSuppRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));*/

    return getRowVector(partSuppRowType,childrenRow);
}

shared_ptr<arrow::RecordBatch> genTpchCustomer(

    size_t maxRows,
    size_t offset,
    double scaleFactor) {
  // Create schema and allocate vectors.
  auto customerRowType = getTableSchema(Table::TBL_CUSTOMER);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_CUSTOMER, scaleFactor), maxRows, offset);
  auto childrenRow = allocateVectors(customerRowType);


    auto custKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto nameVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[1]);
    auto addressVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[2]);
    auto nationKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[3]);
    auto phoneVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[4]);
    auto acctBalVector = dynamic_pointer_cast<arrow::DoubleBuilder>(childrenRow[5]);
    auto mktSegmentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[6]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[7]);

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initCustomer(offset);
  customer_t cust;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.

  arrow::Status status;
  for (size_t i = 0; i < vectorSize; ++i) {
      dbgenIt.genCustomer(i + offset + 1, cust);

      status = custKeyVector->Append(cust.custkey);
      status = nameVector->Append(string(cust.name, strlen(cust.name)));
      status = addressVector->Append(string(cust.address, cust.alen));
      status = nationKeyVector->Append(cust.nation_code);
      status = phoneVector->Append(string(cust.phone, strlen(cust.phone)));
      status = acctBalVector->Append(decimalToDouble(cust.acctbal));
      status = mktSegmentVector->Append(string(cust.mktsegment, strlen(cust.mktsegment)));
      status = commentVector->Append(string(cust.comment, cust.clen));
  }
  /*
  return std::make_shared<RowVector>(
      pool,
      customerRowType,
      BufferPtr(nullptr),
      vectorSize,
      std::move(children));
      */

    return getRowVector(customerRowType,childrenRow);
}

shared_ptr<arrow::RecordBatch> genTpchNation(

    size_t maxRows,
    size_t offset,
    double scaleFactor) {
  // Create schema and allocate vectors.
  auto nationRowType = getTableSchema(Table::TBL_NATION);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_NATION, scaleFactor), maxRows, offset);
  auto childrenRow = allocateVectors(nationRowType);

    auto nationKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto nameVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[1]);
    auto regionKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[2]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[3]);


  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initNation(offset);
  code_t code;

  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.


  arrow::Status status;
  for (size_t i = 0; i < vectorSize; ++i) {
      dbgenIt.genNation(i + offset + 1, code);

      status = nationKeyVector->Append(code.code);
      status = nameVector->Append(string(code.text, strlen(code.text)));
      status = regionKeyVector->Append(code.join);
      status = commentVector->Append(string(code.comment, code.clen));
  }
 // return std::make_shared<RowVector>(
   //   pool, nationRowType, BufferPtr(nullptr), vectorSize, std::move(children));

    return getRowVector(nationRowType,childrenRow);
}

shared_ptr<arrow::RecordBatch>  genTpchRegion(
    size_t maxRows,
    size_t offset,
    double scaleFactor) {
  // Create schema and allocate vectors.
  auto regionRowType = getTableSchema(Table::TBL_REGION);
  size_t vectorSize = getVectorSize(
      getRowCount(Table::TBL_REGION, scaleFactor), maxRows, offset);
  auto childrenRow = allocateVectors(regionRowType);


    auto regionKeyVector = dynamic_pointer_cast<arrow::Int64Builder>(childrenRow[0]);
    auto nameVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[1]);
    auto commentVector = dynamic_pointer_cast<arrow::StringBuilder>(childrenRow[2]);

  DBGenIterator dbgenIt(scaleFactor);
  dbgenIt.initRegion(offset);
  code_t code;


  // Dbgen generates the dataset one row at a time, so we need to transpose it
  // into a columnar format.
  arrow::Status status;
  for (size_t i = 0; i < vectorSize; ++i) {
      dbgenIt.genRegion(i + offset + 1, code);

      status = regionKeyVector->Append(code.code);
      status = nameVector->Append(string(code.text, strlen(code.text)));
      status = commentVector->Append(string(code.comment, code.clen));
  }
 // return std::make_shared<RowVector>(
  //    pool, regionRowType, BufferPtr(nullptr), vectorSize, std::move(children));

    return getRowVector(regionRowType,childrenRow);
}
/*
std::string getQuery(int query) {
  if (query <= 0 || query > TPCH_QUERIES_COUNT) {
  //  VELOX_FAIL("Out of range TPC-H query number {}", query);
  }

  auto queryString = std::string(TPCH_QUERIES[query - 1]);
  // Output of GetQuery() has a new line and a semi-colon. These need to be
  // removed in order to use the query string in a subquery
  queryString.pop_back(); // remove new line
  queryString.pop_back(); // remove semi-colon
  return queryString;
}
*/
} // namespace facebook::velox::tpch
