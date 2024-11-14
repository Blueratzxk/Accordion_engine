//
// Created by zxk on 6/17/23.
//

#ifndef OLVP_CSVTABLEFORMATCHECKER_HPP
#define OLVP_CSVTABLEFORMATCHECKER_HPP

#include "nlohmann/json.hpp"
#include "../../DataSource/SchemaManager.hpp"
#include <iostream>
#include "range/v3/view.hpp"
class CSVTableFormatChecker
{
    CatalogsMetaManager smm;

public:
    CSVTableFormatChecker(){
    }

    void checkDict()
    {
        check("tpch_test","tpch_1","supplier");
        check("tpch_test","tpch_1","nation");
        check("tpch_test","tpch_1","region");
        check("tpch_test","tpch_1","part");
        check("tpch_test","tpch_1","partsupp");
        check("tpch_test","tpch_1","customer");
        check("tpch_test","tpch_1","orders");
        check("tpch_test","tpch_1","lineitem");
    }


    void check(string catalogName,string schemaName,string tableName)
    {
        TableInfo tableInfo = smm.getTable(catalogName,schemaName,tableName);
        if(!TableInfo::isEmpty(tableInfo)) {

            string filePath = tableInfo.getFilePath();
            string firstLine;

            vector<string> columnNames;
            for (int i = 0; i < tableInfo.getColumnNames().size(); i++)
            {
                columnNames.push_back(tableInfo.getColumnNames()[i]);
            }

            if(getFirstLine(filePath,firstLine))
                if(!checkFirstLine(firstLine,columnNames))
                {
                    appendHeader(filePath,columnNames);
                }


        }

    }

    bool getFirstLine(string fileName,string &firstLine)
    {
        ifstream ReadFile;

        ReadFile.open(fileName.c_str());//ios::in 表示以只读的方式读取文件
        if(ReadFile.fail())//文件打开失败:返回0
        {
            return false;
        }
        else//文件存在
        {
            getline(ReadFile,firstLine,'\n');
            ReadFile.close();
            return true;
        }

    }


    void appendHeader(string filePath,vector<string> columns)
    {
        string header;
        for(int i = 0 ; i < columns.size() ; i++)
        {
            header.append(columns[i]);
            header.append("|");
        }
        cout << "File "+filePath+" should append header:"+header<<endl;
    }

    bool checkFirstLine(string line,vector<string> columnNames)
    {
        auto columns = line
                |ranges::views::split('|')
                |ranges::views::transform([](auto&& i){return i | ranges::to<string>();})
                |ranges::to<vector>();

       // cout <<columnNames.size() << columns.size() << "!!!!"<<endl;
        if(columnNames.size() == columns.size())
        {
            for(int i = 0 ; i < columnNames.size(); i++) {
                if (columnNames[i] != columns[i])
                    return false;
            }
        }
        else
            return false;

        return true;
    }




};


#endif //OLVP_CSVTABLEFORMATCHECKER_HPP
