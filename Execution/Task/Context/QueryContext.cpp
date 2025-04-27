//
// Created by zxk on 6/6/23.
//
#include "QueryContext.h"
#include "PipelineContext.h"
#include "TaskContext.h"
#include "DriverContext.h"

QueryContext::QueryContext() {
    this->interTaskDataExchangeManager = make_shared<InterTaskDataExchangeManager>();
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

bool QueryContext::prepareInterTaskDataByComponentId(TaskId taskId,string componentId) {

    string queryId = taskId.getQueryId().getId();
    shared_ptr<TaskContext> context;
    if(this->taskContexts.find(queryId) != this->taskContexts.end()) {
        map<string, shared_ptr<TaskContext>> info;
        info = this->taskContexts[queryId];
        context = info[taskId.ToString()];
    }
    else
        return false;


    auto pipelines = context->getPipelineContexts();
    list<shared_ptr<DriverContext>> allDrivers;
    for (auto pipeline : pipelines)
    {
        for (auto driver :pipeline.second->getDriverContexts())
            allDrivers.push_back(driver);
    }
    for(auto driver : allDrivers)
    {
        shared_ptr<vector<shared_ptr<Operator>>> physicalPipeline;
        if(driver->hasDriver()) {
            physicalPipeline = driver->getDriver();
            for(auto op : *physicalPipeline)
            {
                if(op->getOperatorId() == componentId)
                {
                    return op->externalEvent();
                }
            }
        }
    }
    return false;

}


void QueryContext::inputInterTaskDataByComponentId(TaskId taskId,string componentId,vector<shared_ptr<DataPage>> pages) {

    string queryId = taskId.getQueryId().getId();
    shared_ptr<TaskContext> context;
    if(this->taskContexts.find(queryId) != this->taskContexts.end()) {
        map<string, shared_ptr<TaskContext>> info;
        info = this->taskContexts[queryId];
        context = info[taskId.ToString()];
    }
    else
        return;


    auto pipelines = context->getPipelineContexts();
    list<shared_ptr<DriverContext>> allDrivers;
    for (auto pipeline : pipelines)
    {
        for (auto driver :pipeline.second->getDriverContexts())
            allDrivers.push_back(driver);
    }
    for(auto driver : allDrivers)
    {
        shared_ptr<vector<shared_ptr<Operator>>> physicalPipeline;
        if(driver->hasDriver()) {
            physicalPipeline = driver->getDriver();
            for(auto op : *physicalPipeline)
            {
                if(op->getOperatorId() == componentId)
                {
                    op->fulfillExternalEventWithPages(pages);
                }
            }
        }
    }

}