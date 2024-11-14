//
// Created by zxk on 11/25/23.
//

#ifndef OLVP_RUNTIMECONFIGPARSER_HPP
#define OLVP_RUNTIMECONFIGPARSER_HPP

#include <string>
#include <map>
#include <vector>
#include <set>
using namespace std;

class RuntimeConfigParser
{
    multimap<string,vector<string>> runtimeConfigs;
    set<string> configTypes = {"SET_TABLE_SCAN_SIZE"};
public:
    RuntimeConfigParser(multimap<string,vector<string>> runtimeConfigs){
        this->runtimeConfigs = runtimeConfigs;

    }

    string findTableScanSizeConfig(string tableName)
    {
        for(auto map : this->runtimeConfigs)
        {
            if(map.first == "SET_TABLE_SCAN_SIZE")
            {
                vector<string> paras;
                paras = map.second;
                if(paras.size() != 2)
                    return "NULL";

                string tname = paras[0];
                string size = paras[1];
                if(tname == tableName)
                    return size;
            }
        }
        return "NULL";
    }


};


#endif //OLVP_RUNTIMECONFIGPARSER_HPP
