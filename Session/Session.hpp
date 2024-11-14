//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_SESSION_HPP
#define OLVP_SESSION_HPP

#include "../common.h"
#include "../Config/ExecutionConfig.hpp"
#include "SessionRepresentation.hpp"

using namespace std;
class Session
{
    string queryId;
    int stageExecutionId;
    shared_ptr<ExecutionConfig> executionConfig = make_shared<ExecutionConfig>();
    multimap<string,vector<string>> runtimeConfigs;



public:
    Session(string queryId,int stageExecutionId)
    {
        this->queryId = queryId;
        this->stageExecutionId = stageExecutionId;
    }


    void setRumtimeConfigs(multimap<string,vector<string>> configs)
    {
        this->runtimeConfigs = configs;
    }
    string getQueryId()
    {
        return this->queryId;
    }
    int getStageExecutionId()
    {
        return this->stageExecutionId;
    }

    shared_ptr<ExecutionConfig> getExecutionConfig()
    {
        return this->executionConfig;
    }

    shared_ptr<SessionRepresentation> toSessionRepresentation()
    {
        return make_shared<SessionRepresentation>(this->queryId,this->runtimeConfigs);
    }
    RuntimeConfigParser getRuntimeConfigs()
    {
        return RuntimeConfigParser(this->runtimeConfigs);
    }

};


#endif //OLVP_SESSION_HPP
