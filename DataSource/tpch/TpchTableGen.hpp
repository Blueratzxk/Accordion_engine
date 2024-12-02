//
// Created by zxk on 11/30/24.
//

#ifndef TPCHDBGEN_TPCHTABLEGEN_HPP
#define TPCHDBGEN_TPCHTABLEGEN_HPP
#include <iostream>
#include <utility>

#include "../tpch/TpchGen.h"

#include "../../Connector/ConnectorPageSource.hpp"
class TpchTableGen : public ConnectorPageSource
{
    enum Tables
    {
        TPCH_LINEITEM,
        TPCH_ORDERS,
        TPCH_SUPPLIER,
        TPCH_NATION,
        TPCH_REGION,
        TPCH_CUSTOMER,
        TPCH_PART,
        TPCH_PARTSUPP,
        UNKNOWN
    };
    int SplitTupleCount;
    int SplitOffset;
    int scanBatchSize;
    int scaleFactor;
    string table;

    Tables curTable;

    int curIndex = 0;

    shared_ptr<arrow::RecordBatch> nextBatch = NULL;

    int lineitem_ordersMove = 0;

    void genBatch()
    {
        if(curIndex >= SplitTupleCount) {
            this->nextBatch = NULL;
            return;
        }

        int scanCount = scanBatchSize;
        if(curIndex + scanCount > SplitTupleCount)
            scanCount = SplitTupleCount - curIndex;



        switch (this->curTable) {
            case TPCH_LINEITEM:
                this->nextBatch = facebook::velox::tpch::genTpchLineItem(scanCount/4,curIndex+SplitOffset, scaleFactor, &lineitem_ordersMove);
                break;
            case TPCH_CUSTOMER:
                this->nextBatch = facebook::velox::tpch::genTpchCustomer(scanCount,curIndex+SplitOffset, scaleFactor);
                break;
            case TPCH_NATION:
                this->nextBatch = facebook::velox::tpch::genTpchNation(scanCount,curIndex+SplitOffset, scaleFactor);
                break;
            case TPCH_ORDERS:
                this->nextBatch = facebook::velox::tpch::genTpchOrders(scanCount,curIndex+SplitOffset, scaleFactor);
                break;
            case TPCH_PART:
                this->nextBatch = facebook::velox::tpch::genTpchPart(scanCount,curIndex+SplitOffset, scaleFactor);
                break;
            case TPCH_PARTSUPP:
                this->nextBatch = facebook::velox::tpch::genTpchPartSupp(scanCount,curIndex+SplitOffset, scaleFactor);
                break;
            case TPCH_REGION:
                this->nextBatch = facebook::velox::tpch::genTpchRegion(scanCount,curIndex+SplitOffset, scaleFactor);
                break;
            case TPCH_SUPPLIER:
                this->nextBatch = facebook::velox::tpch::genTpchSupplier(scanCount,curIndex+SplitOffset, scaleFactor);
                break;
        }

    }

public:
    TpchTableGen(int SplitTupleCount,int SplitOffset,int scanBatchSize,int scaleFactor,string tableName) : ConnectorPageSource("TpchTableGen"){
        this->SplitTupleCount = SplitTupleCount;
        this->SplitOffset = SplitOffset;
        this->scanBatchSize = scanBatchSize;
        this->scaleFactor = scaleFactor;
        this->table = std::move(tableName);

        if (table == "LINEITEM" || table == "lineitem")
            curTable = TPCH_LINEITEM;
        else if (table == "ORDERS" || table == "orders")
            curTable = TPCH_ORDERS;
        else if (table == "SUPPLIER" || table == "supplier")
            curTable = TPCH_SUPPLIER;
        else if (table == "NATION" || table == "nation")
            curTable = TPCH_NATION;
        else if (table == "REGION" || table == "region")
            curTable = TPCH_REGION;
        else if (table == "CUSTOMER" || table == "customer")
            curTable = TPCH_CUSTOMER;
        else if (table == "PART" || table == "part")
            curTable = TPCH_PART;
        else if (table == "PARTSUPP" || table == "partsupp")
            curTable = TPCH_PARTSUPP;
        else
            curTable = UNKNOWN;
    }


    shared_ptr<arrow::RecordBatch> getNextBatch() override
    {

        genBatch();
        if(this->nextBatch != NULL) {
            if(curTable == TPCH_LINEITEM)
                this->curIndex += lineitem_ordersMove;
            else
                this->curIndex += this->nextBatch->column(0)->length();
        }
        return this->nextBatch;
    }

    size_t getBytesRead() override
    {
        return this->curIndex;
    }
    size_t getFileBytesSize() override
    {
        return this->SplitTupleCount;
    }

    string getProgress()
    {
        return to_string(((double)curIndex)/((double)SplitTupleCount) * 100.0)+"%";
    }

};

#endif //TPCHDBGEN_TPCHTABLEGEN_HPP
