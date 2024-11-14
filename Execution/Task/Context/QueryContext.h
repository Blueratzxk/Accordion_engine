//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_QUERYCONTEXT_H
#define OLVP_QUERYCONTEXT_H


#include <map>
#include "../../../common.h"
//#include "../Id/TaskId.hpp"
#include "../../../Session/RuntimeConfigParser.hpp"

class TaskContext;
using namespace std;
class QueryContext:public std::enable_shared_from_this<QueryContext>
{

    map<string,map<string,shared_ptr<TaskContext>>> taskContexts;
    shared_ptr<RuntimeConfigParser> runtimeConfigs = NULL;
public:
    QueryContext();
    void addTaskContext(string queryId,string taskId,shared_ptr<TaskContext> taskContext);
    map<string,shared_ptr<TaskContext>> getTaskContexts(string queryId);
    map<string,set<shared_ptr<TaskContext>>> getAllRunningTaskContexts();
    shared_ptr<TaskContext> getTaskContext(string QueryId,string taskId);
    shared_ptr<RuntimeConfigParser> getRuntimeConfigs();


};


#endif //OLVP_QUERYCONTEXT_H
