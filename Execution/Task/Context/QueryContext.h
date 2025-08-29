//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_QUERYCONTEXT_H
#define OLVP_QUERYCONTEXT_H


#include <map>
#include "../../../common.h"
//#include "../Id/TaskId.hpp"
#include "../../../Session/RuntimeConfigParser.hpp"
#include "../SidewayExchangeSystem/InterTaskDataExchangeManager.hpp"

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

    void saveInterTaskPages(string sourceId,vector<shared_ptr<DataPage>> pages)
    {
        this->interTaskDataExchangeManager->savePages(sourceId,pages);
    }

    void releaseRemoteInterTaskDataFetcher(string taskId,string targetId,string ip,string port,string sourceId, string bufferId)
    {
        thread executor(requestRemoteInterTaskData,shared_from_this(),taskId,targetId,ip,port,sourceId,bufferId);
        executor.detach();
    }
    static vector<shared_ptr<DataPage>> requestRemoteInterTaskData(shared_ptr<QueryContext> queryContext,string curTaskId,string targetId,string sourceIp,string sourcePort,string sourceId,string bufferId)
    {
        auto result = queryContext->interTaskDataExchangeManager->requestRemoteInterTaskPages(curTaskId,sourceIp,sourcePort,sourceId,bufferId);
        TaskId tId;
        queryContext->inputInterTaskDataByComponentId(*tId.StringToObject(curTaskId),targetId,result);
        return result;
    }


    bool prepareInterTaskDataByComponentId(TaskId taskId,set<string> sourceTypes,map<string,set<string>> &sourceIdMap);
    void inputInterTaskDataByComponentId(TaskId taskId,string componentId,vector<shared_ptr<DataPage>> pages);
};


#endif //OLVP_QUERYCONTEXT_H
