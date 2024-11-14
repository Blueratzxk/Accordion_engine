//
// Created by zxk on 5/15/23.
//

#ifndef OLVP_DATAFILEDICTS_HPP
#define OLVP_DATAFILEDICTS_HPP
//#include "../common.h"
#include<fstream>
using namespace std;


class TableInfo
{
    string tableName;
    string fileType;
    string filePath;
    map<int,string> columnNames;
    map<int,string> columnTypes;
    vector<pair<string,string>> partiionedFilePaths;
    string defaultBlockScanSize;

public:
    TableInfo(){}
    TableInfo(string tableName){ this->tableName = tableName;}
    TableInfo(string tableName,string fileType,string filePath,vector<pair<string,string>> partiionedFilePaths,map<int,string> columnNames,map<int,string> columnTypes,string defaultBlockScanSize)
    {
        this->tableName = tableName;
        this->fileType = fileType;
        this->filePath = filePath;
        this->columnNames = columnNames;
        this->columnTypes = columnTypes;
        this->partiionedFilePaths = partiionedFilePaths;
        this->defaultBlockScanSize = defaultBlockScanSize;
    }
    string getTableName()
    {
        return this->tableName;
    }

    string getDefaultBlockScanSize()
    {
        return this->defaultBlockScanSize;
    }
    string getFileType()
    {
        return this->fileType;
    }
    string getFilePath()
    {
        return this->filePath;
    }
    map<int,string>  getColumnNames()
    {
        return this->columnNames;
    }
    map<int,string> ColumnTypes()
    {
        return this->columnTypes;
    }
    vector<pair<string,string>> getPartiionedFilePaths()
    {
        return this->partiionedFilePaths;
    }

    string to_string()
    {

        string all;

        string columns;
        for(auto col : this->columnNames)
        {
            columns+=col.second+"|";
        }

        string types;
        for(auto col : this->columnTypes)
        {
            types+=col.second+"|";
        }
        all += this->tableName;
        all += "\n";
        all += this->filePath;
        all += "\n";
        all += this->fileType;
        all += "\n";

        for(int i = 0 ; i < this->partiionedFilePaths.size() ; i++) {
            all.append(this->partiionedFilePaths[i].first);
            all.append("|");
            all.append(this->partiionedFilePaths[i].second);
            all.append("|");
        }


        all += "\n";
        all += columns;
        all += "\n";
        all += types;
        all += "\n";

        return all;
    }

    static TableInfo getEmptyTableInfo()
    {
        return TableInfo("EMPTY");
    }
    static bool isEmpty(TableInfo ti)
    {
        if(ti.tableName.compare("EMPTY") == 0)
            return true;
        else
            return false;
    }


};

class Schema
{
    string schemaName;
    map<string,TableInfo> tables;
    string catalogType;
public:
    Schema(){}
    Schema(string schemaName,string catalogType,map<string,TableInfo> tables)
    {
        this->catalogType = catalogType;
        this->schemaName = schemaName;
        this->tables = tables;
    }
    string getSchemaName()
    {
        return this->schemaName;
    }
    map<string,TableInfo> getTables()
    {
        return this->tables;
    }
    string getCatalogType()
    {
        return this->catalogType;
    }

    string to_string()
    {
        string all;

        all += schemaName+="\n";

        for(auto table : tables)
        {
            all += table.second.to_string();
        }
        return all;
    }

    static Schema getEmptySchema()
    {
        map<string,TableInfo> tables;
        return Schema("EMPTY", "EMPTY",tables);
    }
    static bool isEmpty(Schema schema)
    {
        if(schema.schemaName.compare("EMPTY") == 0)
        {
            return true;
        }
        else
            return false;
    }

};



class DataFileDicts
{
    string dictPath = "DataFileDicts";

    nlohmann::json jsonTree;

public:
    DataFileDicts(){}

    bool readDictFile()
    {
        std::ifstream file(dictPath, std::ios::in | std::ios::binary);
        if (!file.is_open())
        {
            spdlog::warn("Cannot open the Dict file!");
            file.close();
            return false;
        }
        jsonTree = nlohmann::json::parse(file);
        file.close();
        return  true;
    }



    map<string,TableInfo> parseTables(nlohmann::json tableInfoArray)
    {
        map<string,TableInfo> tableInfoMap;
        for(int i = 0 ; i < tableInfoArray.size() ; i++) {
            string tableName = tableInfoArray[i]["tableName"];
            string fileType = tableInfoArray[i]["fileType"];
            string filePath = tableInfoArray[i]["filePath"];
            vector<string> columnNames = tableInfoArray[i]["columnNames"];

            map<int, string> columnNamesMap;
            map<int, string> columnTypesMap;

            for (int index = 0; index < columnNames.size(); index++) {
                columnNamesMap[index] = columnNames[index];
            }
            vector<string> columnTypes = tableInfoArray[i]["columnTypes"];

            for (int index = 0; index < columnTypes.size(); index++) {
                columnTypesMap[index] = columnTypes[index];
            }

            vector<pair<string,string>> pfs;
            nlohmann::json partitionedFilePaths = nlohmann::json::array();
            partitionedFilePaths = tableInfoArray[i]["distributedFilePaths"];

            for(auto object : partitionedFilePaths)
            {
                auto info = make_pair<string,string>(object["netAddr"],object["fileAddr"]);
                pfs.push_back(info);
            }

            string defaultBlockScanSize = tableInfoArray[i]["defaultBlockScanSize"];
            TableInfo ti(tableName, fileType, filePath, pfs,columnNamesMap, columnTypesMap,defaultBlockScanSize);

            if (tableInfoMap.count(tableName) == 0) {
                tableInfoMap[tableName] = ti;
            } else {
                spdlog::critical("Dict Error! Mul Table Def!");
                tableInfoMap.clear();
                return tableInfoMap;
            }
        }

        return tableInfoMap;

    }

    map<string,Schema> parseCatalogs()
    {
        map<string,Schema> Catalogs;

        if(readDictFile())
        {

            for (const auto &item : jsonTree.items())
            {
                if (!item.key().empty() && jsonTree[item.key()].is_object())
                {
                    string catalogName  = item.key();
                    nlohmann::json schema = jsonTree[item.key()];

                    string schemaName = schema["schemaName"];
                    nlohmann::json tables = schema["tables"];
                    string catalogType = schema["catalogType"];

                    if(Catalogs.count(catalogName) == 0)
                        Catalogs[catalogName] = Schema(schemaName,catalogType,this->parseTables(tables));
                    else
                    {
                        spdlog::critical("Schema ERROR! Same Schema name!");
                        Catalogs.clear();
                        return Catalogs;
                    }
                }
            }

        }

        return Catalogs;
    }

};




#endif //OLVP_DATAFILEDICTS_HPP
