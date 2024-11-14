//
// Created by zxk on 6/18/23.
//

#ifndef OLVP_TABLESCANDESCRIPTOR_HPP
#define OLVP_TABLESCANDESCRIPTOR_HPP

#include <string>
#include "nlohmann/json.hpp"
using namespace std;

class TableScanDescriptor
{

    string catalog;
    string schema;
    string table;

public:
    TableScanDescriptor(){}
    TableScanDescriptor(  string catalog,string schema,string table)
    {
        this->catalog = catalog;
        this->schema = schema;
        this->table = table;
    }

    string getCatalog(){return this->catalog;}
    string getSchema(){return this->schema;}
    string getTable(){return this->table;}

    static string Serialize(TableScanDescriptor tableScanDescriptor)
    {
        nlohmann::json tableScan;

        tableScan["catalog"] = tableScanDescriptor.catalog;
        tableScan["schema"] = tableScanDescriptor.schema;
        tableScan["table"] = tableScanDescriptor.table;

        string result = tableScan.dump();

        return result;
    }


    static TableScanDescriptor Deserialize(string desc)
    {
        nlohmann::json tableScan = nlohmann::json::parse(desc);
        return TableScanDescriptor(tableScan["catalog"],tableScan["schema"],tableScan["table"]);
    }



};



#endif //OLVP_TABLESCANDESCRIPTOR_HPP
