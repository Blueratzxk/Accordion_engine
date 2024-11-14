//
// Created by zxk on 5/15/23.
//

#ifndef OLVP_SCHEMAMANAGER_HPP
#define OLVP_SCHEMAMANAGER_HPP

#include "DataFileDicts.hpp"



class CatalogsMetaManager
{
    map<string,Schema> catalogs;
    DataFileDicts DDicts;

public:
    CatalogsMetaManager()
    {
        this->catalogs = DDicts.parseCatalogs();
    }

    Schema getSchema(string catalogName,string SchemaName)
    {
        if(catalogs.count(catalogName) != 0)
        {
            if(catalogs[catalogName].getSchemaName().compare(SchemaName) == 0)
                return catalogs[catalogName];
            else
                return Schema::getEmptySchema();
        }
        else
        {
            return Schema::getEmptySchema();
        }
    }

    TableInfo getTable(string catalogName,string schemaName,string TableName)
    {
        if(Schema::isEmpty(getSchema(catalogName,schemaName)))
        {
            return TableInfo::getEmptyTableInfo();
        }
        Schema schema = this->catalogs[catalogName];
        if(schema.getTables().count(TableName) == 0)
        {
            return TableInfo::getEmptyTableInfo();
        }
        else
        {
            return schema.getTables()[TableName];
        }

    }

    list<string> showTables()
    {
        list<string> tableNames;
        auto schema = getSchema("tpch_test","tpch_1");
        auto tables = schema.getTables();
        for(auto table : tables)
            tableNames.push_back(table.first);

        return tableNames;
    }

    string describeTable(string tableName) {
        auto schema = getSchema("tpch_test", "tpch_1");
        auto tables = schema.getTables();

        if(!tables.contains(tableName))
            return "No table found!";

        auto tableInfo = tables[tableName];

        string output;
        auto colNames = tableInfo.getColumnNames();
        auto colTypes = tableInfo.ColumnTypes();

        for(int i = 0 ; i < tableInfo.getColumnNames().size() ; i++)
        {
            output.append(colNames[i]);
            output.append("\t| ");
            output.append(colTypes[i]);
            output.append("\n");
        }

        return output;

    }

    void viewSchemas()
    {
        for(auto schema : catalogs)
        {
            cout << schema.first << endl;
            cout << schema.second.to_string() << endl;
        }
    }

};


#endif //OLVP_SCHEMAMANAGER_HPP
