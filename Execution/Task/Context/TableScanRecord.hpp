//
// Created by zxk on 6/11/23.
//

#ifndef OLVP_TABLESCANRECORD_HPP
#define OLVP_TABLESCANRECORD_HPP

#include <iostream>
#include <atomic>
#include <memory>
#include "nlohmann/json.hpp"
using namespace std;
class TableScanRecord
{
    atomic<size_t> scanedBytes = 0;
    atomic<size_t> totalTableBytes = 0;
    string catalogName;
    string schemaName;
    string tableName;
    string tableScanId;
    bool tpchAutoGen = false;
    int scaleFactor = -1;

public:
    TableScanRecord(string tableScanId,string catalogName,string schemaName,string tableName,
                    size_t scanedBytes,size_t totalTableBytes,bool tpchAutoGen = false, int scaleFactor = -1)
    {
        this->catalogName = catalogName;
        this->schemaName = schemaName;
        this->tableName = tableName;
        this->scanedBytes = scanedBytes;
        this->totalTableBytes = totalTableBytes;
        this->tableScanId = tableScanId;
        this->tpchAutoGen = tpchAutoGen;
        this->scaleFactor = scaleFactor;
    }
    size_t getTotalTableBytes()
    {
        return this->totalTableBytes;
    }
    size_t getScanedBytes()
    {
        return this->scanedBytes;
    }

    bool getTpchAutoGen(){return this->tpchAutoGen;}
    int getScaleFactor(){return this->scaleFactor;}

    void updateScanedBytes(size_t scaned)
    {
        this->scanedBytes = scaned;
    }

    string getCatalogName()
    {
        return this->catalogName;
    }
    string getSchemaName()
    {
        return this->schemaName;
    }
    string getTableName()
    {
        return this->tableName;
    }
    string getTableScanId()
    {
        return this->tableScanId;
    }

    string ToString()
    {

        string result;
        result.append("{");
        result.append("\"tableScanId\":");
        result.append("\""+this->tableScanId+"\"");
        result.append(",");
        result.append("\"catalogName\":");
        result.append("\""+this->catalogName+"\"");
        result.append(",");
        result.append("\"schemaName\":");
        result.append("\""+this->schemaName+"\"");
        result.append(",");
        result.append("\"tableName\":");
        result.append("\""+this->tableName+"\"");
        result.append(",");
        result.append("\"scanedBytes\":");
        result.append("\""+ to_string(this->scanedBytes)+"\"");
        result.append(",");
        result.append("\"totalTableBytes\":");
        result.append("\""+ to_string(this->totalTableBytes)+"\"");
        result.append(",");
        result.append("\"tpchAutoGen\":");
        result.append("\""+ to_string(this->tpchAutoGen)+"\"");
        result.append(",");
        result.append("\"scaleFactor\":");
        result.append("\""+ to_string(this->scaleFactor)+"\"");
        result.append("}");


        return result;

    }


    static string Serialize(shared_ptr<TableScanRecord> tableScanRecord)
    {
        nlohmann::json json;

        json["tableScanId"] = tableScanRecord->tableScanId;
        json["scanedBytes"] = (u_int64_t)tableScanRecord->scanedBytes;
        json["totalTableBytes"] = (u_int64_t)tableScanRecord->totalTableBytes;
        json["catalogName"] = tableScanRecord->catalogName;
        json["schemaName"] = tableScanRecord->schemaName;
        json["tableName"] = tableScanRecord->tableName;
        json["tpchAutoGen"] = tableScanRecord->tpchAutoGen;
        json["scaleFactor"] = tableScanRecord->scaleFactor;

        string result = json.dump();
        return result;
    }

    static shared_ptr<TableScanRecord> Deserialize(string tableScanRecord)
    {
        nlohmann::json json = nlohmann::json::parse(tableScanRecord);
        u_int64_t scanedBytesJson = json["scanedBytes"];
        u_int64_t totalTableBytesJson = json["totalTableBytes"];

        return make_shared<TableScanRecord>(json["tableScanId"],json["catalogName"],json["schemaName"],
                                            json["tableName"],(size_t)scanedBytesJson,(size_t)totalTableBytesJson
                                            ,json["tpchAutoGen"],json["scaleFactor"]);
    }




};


#endif //OLVP_TABLESCANRECORD_HPP
