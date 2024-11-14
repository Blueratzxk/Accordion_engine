//
// Created by zxk on 11/18/23.
//

#ifndef OLVP_EXECUTIONCONTEXT_HPP
#define OLVP_EXECUTIONCONTEXT_HPP

#include <string>
using namespace std;
class ScriptExecutionContext
{
    string queryId = "";
    multimap<string,vector<string>> runtimeConfigs;
public:
    ScriptExecutionContext()
    {

    }

    void addConfig(string configType,vector<string> paras)
    {
        this->runtimeConfigs.insert({configType, paras});
    }
    multimap<string,vector<string>> getRuntimeConfigs()
    {
        return this->runtimeConfigs;
    }

    bool setQueryId(string id)
    {
        if(this->queryId == "") {
            this->queryId = id;
            return true;
        }
        else {
            spdlog::error("Trying to start multi-queries in a script.");
            return false;
        }
    }

    bool getQueryId(string &id)
    {
        id = this->queryId;
        if(this->queryId == "") {
            return false;
        }
        else
            return true;
    }



};
#endif //OLVP_EXECUTIONCONTEXT_HPP
