//
// Created by zxk on 6/6/23.
//
#include "QueryContext.h"
#include "TaskContext.h"

QueryContext::QueryContext() {

}

void QueryContext::addTaskContext(string queryId,string taskId, shared_ptr<TaskContext> taskContext) {

    if(this->taskContexts.find(queryId) == this->taskContexts.end()) {
        map<string,shared_ptr<TaskContext>> info;
        info[taskId] = taskContext;
        this->taskContexts[queryId] = info;
    }
    else {
        if (this->taskContexts[queryId].find(taskId) == this->taskContexts[queryId].end())
            this->taskContexts[queryId][taskId] = taskContext;
    }
}

map<string, shared_ptr<TaskContext>> QueryContext::getTaskContexts(string QueryId) {
    return this->taskContexts[QueryId];
}
map<string,set<shared_ptr<TaskContext>>> QueryContext::getAllRunningTaskContexts() {



    map<string,set<shared_ptr<TaskContext>>> re;

    for(auto query : this->taskContexts)
    {
        set<shared_ptr<TaskContext>> tcs;
        for(auto tc : query.second) {
            if (tc.second->getState()->isRunning())
                tcs.insert(tc.second);
        }
        re[query.first] = tcs;
    }
    return re;
}
shared_ptr<TaskContext> QueryContext::getTaskContext(string QueryId,string taskId) {

    if(this->taskContexts.find(QueryId) != this->taskContexts.end()) {
        if (this->taskContexts[QueryId].find(taskId) == this->taskContexts[QueryId].end())
            return this->taskContexts[QueryId][taskId];
        else
            return NULL;
    }
    else
        return NULL;

}

shared_ptr<RuntimeConfigParser> QueryContext::getRuntimeConfigs()
{
    return this->runtimeConfigs;
}