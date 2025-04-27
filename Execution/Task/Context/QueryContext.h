//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_QUERYCONTEXT_H
#define OLVP_QUERYCONTEXT_H


#include <map>
#include "../../../common.h"
//#include "../Id/TaskId.hpp"
#include "../../../Session/RuntimeConfigParser.hpp"
#include "../InterTaskDataExchangeManager.hpp"

class TaskContext;
using namespace std;
class QueryContext:public std::enable_shared_from_this<QueryContext>
{

    map<string,map<string,shared_ptr<TaskContext>>> taskContexts;
    shared_ptr<RuntimeConfigParser> runtimeConfigs = NULL;

    shared_ptr<InterTaskDataExchangeManager> interTaskDataExchangeManager;
public:
    QueryContext();
    void addTaskContext(string queryId,string taskId,shared_ptr<TaskContext> taskContext);
    map<string,shared_ptr<TaskContext>> getTaskContexts(string queryId);
    map<string,set<shared_ptr<TaskContext>>> getAllRunningTaskContexts();
    shared_ptr<TaskContext> getTaskContext(string QueryId,string taskId);
    shared_ptr<RuntimeConfigParser> getRuntimeConfigs();

    vector<shared_ptr<DataPage>> takeInterTaskPages(string componentId,string bufferId)
    {
        return this->interTaskDataExchangeManager->takePages(componentId,bufferId,1);
    }

    void saveInterTaskPages(string componentId,vector<shared_ptr<DataPage>> pages)
    {
        this->interTaskDataExchangeManager->savePages(componentId,pages);
    }

    void releaseRemoteInterTaskDataFetcher(string taskId,string componentId, string bufferId,string ip,string port)
    {
        thread executor(requestRemoteInterTaskData,shared_from_this(),taskId,componentId,bufferId,ip,port);
        executor.detach();
    }
    static vector<shared_ptr<DataPage>> requestRemoteInterTaskData(shared_ptr<QueryContext> queryContext,string taskId,string componentId, string bufferId,string ip,string port)
    {
        auto result = queryContext->interTaskDataExchangeManager->requestRemoteInterTaskPages(taskId,componentId,bufferId,ip,port);
        TaskId tId;
        queryContext->inputInterTaskDataByComponentId(*tId.StringToObject(taskId),componentId,result);
        return result;
    }


    bool prepareInterTaskDataByComponentId(TaskId taskId,string componentId);
    void inputInterTaskDataByComponentId(TaskId taskId,string componentId,vector<shared_ptr<DataPage>> pages);
};


#endif //OLVP_QUERYCONTEXT_H
