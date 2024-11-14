//
// Created by zxk on 5/17/23.
//

#ifndef OLVP_TPCHTABLEHANDLE_HPP
#define OLVP_TPCHTABLEHANDLE_HPP

#include <string>
#include "../Connector/ConnectorTableHandle.hpp"
using namespace std;

class TpchTableHandle:public ConnectorTableHandle
{

    string catalogName;
    string schemaName;
    string tableName;

public:
    TpchTableHandle(string catalogName,string schemaName,string tableName): ConnectorTableHandle("TpchTableHandle")
    {
        this->catalogName = catalogName;
        this->schemaName = schemaName;
        this->tableName = tableName;
    }

    string getSchemaName()
    {
        return this->schemaName;
    }
    string getTableName()
    {
        return this->tableName;
    }

    string getCatalogName()
    {
        return this->catalogName;
    }

    static string Serialize(TpchTableHandle tableHandle)
    {
        nlohmann::json tpch;

        tpch["catalogName"] = tableHandle.catalogName;
        tpch["schemaName"] = tableHandle.schemaName;
        tpch["tableName"] = tableHandle.tableName;

        string result = tpch.dump();
        return result;

    }
    static shared_ptr<TpchTableHandle> Deserialize(string tpchHandle)
    {
        nlohmann::json tpch = nlohmann::json::parse(tpchHandle);
        return make_shared<TpchTableHandle>(tpch["catalogName"],tpch["schemaName"],tpch["tableName"]);
    }

};

#endif //OLVP_TPCHTABLEHANDLE_HPP
