//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_REMOTETASK_HPP
#define OLVP_REMOTETASK_HPP

#include <atomic>
//#include "../../common.h"
#include "../../Execution/Buffer/OutputBufferSchema.hpp"
#include "../../Web/Restful/Client.hpp"

#include "../../Planner/Fragment.hpp"
#include "Id/TaskId.hpp"
#include "TaskSource.hpp"
#include "../../Descriptor/TaskInterfere/TaskIntraParaUpdateRequest.hpp"

#include "../../Descriptor/InterTaskMissionDescriptor.hpp"


#include "../../Descriptor/TaskUpdateRequest.hpp"
#include "Fetcher/TaskInfoFetcher.hpp"

#include "../Event/Event.h"

class HttpRemoteTask
{
    shared_ptr<TaskId> taskId;
    shared_ptr<PlanFragment> fragment;
    shared_ptr<TaskInfoFetcher> taskInfoFetcher;

    string httpRequestLocation;
    string rpcLocation;

    shared_ptr<OutputBufferSchema> schema = NULL;
    shared_ptr<TaskSource> initial_taskSource;
    atomic<bool> sendPlan = false;
    atomic<bool> started = false;
    atomic<bool> needsUpdate = false;
    shared_ptr<Session> session;

    shared_ptr<Event> eventListener;

    shared_ptr<RestfulClient> restfulClient;

    set<string> extension;

    shared_ptr<TaskExecutionCondition> condition;
    bool getFinalTaskInfo = false;

    bool migratedBufferTask = false;

    bool isAbandoned = false;

    int parallelismFactor = 1;

    long totalTupleBytes = 0;

public:
    HttpRemoteTask(shared_ptr<Event> eventListener,shared_ptr<TaskId> taskId,shared_ptr<PlanFragment> fragment,string nodeLocation,
                   shared_ptr<OutputBufferSchema> schema,shared_ptr<TaskSource> initial_taskSources,shared_ptr<Session> session,
                   set<string> extension,shared_ptr<TaskExecutionCondition> condition){
        this->taskId = taskId;
        this->httpRequestLocation = nodeLocation;
        this->schema = schema;
        this->initial_taskSource = initial_taskSources;
        this->eventListener = eventListener;
        this->fragment = fragment;
        this->session = session;
        this->extension = extension;
        this->condition = condition;
        this->taskInfoFetcher = make_shared<TaskInfoFetcher>(this->taskId,this->httpRequestLocation,this->eventListener);
        this->restfulClient = make_shared<RestfulClient>();

    }

    void updateParallelismFactor(int factor)
    {
        if(factor <= 0)
            this->parallelismFactor = 1;
        else
            this->parallelismFactor = factor;
    }
    int getParallelismFactor()
    {
        return this->parallelismFactor;
    }

    void setGetFinalTaskInfo()
    {
        this->getFinalTaskInfo = true;
    }
    bool isGetFinalTaskInfo(){
        return this->getFinalTaskInfo;
    }

    void setMigratedBufferTask()
    {
        this->migratedBufferTask = true;
    }

    bool isMigratedBufferTask()
    {
        return this->migratedBufferTask;
    }

    bool isAbandonedTask()
    {
        return this->isAbandoned;
    }
    void abandonTask()
    {
        this->isAbandoned = true;
    }

    shared_ptr<TaskId> getTaskId()
    {
        return this->taskId;
    }
    string getTaskLocation()
    {
        return this->httpRequestLocation;
    }
    string getRPCLocation()
    {
        return this->rpcLocation;
    }

    shared_ptr<TaskInfoFetcher> getTaskInfoFetcher()
    {
        return this->taskInfoFetcher;
    }

    void splitString(const std::string& s, std::vector<std::string>& tokens, const std::string& delimiters = " ")
    {
        std::string::size_type lastPos = s.find_first_not_of(delimiters, 0);
        std::string::size_type pos = s.find_first_of(delimiters, lastPos);
        while (std::string::npos != pos || std::string::npos != lastPos) {
            tokens.push_back(s.substr(lastPos, pos - lastPos));
            lastPos = s.find_first_not_of(delimiters, pos);
            pos = s.find_first_of(delimiters,lastPos);
        }
    }

    bool isDone()
    {
        return this->taskInfoFetcher->isDone();
    }

    string getIP()
    {
        vector<string> tokens;
        splitString(this->httpRequestLocation,tokens,":");
        return tokens[0];
    }
    string getPORT()
    {
        vector<string> tokens;
        splitString(this->httpRequestLocation,tokens,":");
        return tokens[1];
    }

    set<string> getExtensions()
    {
        return this->extension;
    }
    bool hasThroughput()
    {
        if(this->taskInfoFetcher->isDone())
            return false;
        else
            return this->taskInfoFetcher->taskHasThroughput();
    }
    string getRemoteTaskStatus()
    {
        return "";
    }
    void start(){
        this->started = true;
        createTask();
        this->taskInfoFetcher->start();
    }
    void createTask()
    {
        TaskUpdateRequest request(this->initial_taskSource,this->schema,this->fragment,NULL,this->session->toSessionRepresentation(),this->condition);
        scheduleUpdate(TaskUpdateRequest::Serialize(request),"/v1/task/updateTask");
    }

    void addSplits(shared_ptr<TaskSource> tss)
    {

        TaskUpdateRequest updateRequest(tss,this->schema);
        scheduleUpdate(TaskUpdateRequest::Serialize(updateRequest),"/v1/task/updateTask");
    }
    void updateTaskIntraParallelism(shared_ptr<TaskIntraParaUpdateRequest> intraParaUpdateRequest)
    {
        TaskUpdateRequest updateRequest(intraParaUpdateRequest);
        scheduleUpdate(TaskUpdateRequest::Serialize(updateRequest),"/v1/task/updateTask");
    }
    void setOutputBuffers(shared_ptr<OutputBufferSchema> newSchema)
    {
        this->schema = newSchema;
        if(this->migratedBufferTask)
            this->schema->setMigratedBuffer();

        TaskUpdateRequest updateRequest(this->schema);
        scheduleUpdate(TaskUpdateRequest::Serialize(updateRequest),"/v1/task/updateTask");
    }
    void operateOutputBuffer(shared_ptr<TaskBufferOperatingRequest> taskBufferOperatingRequest)
    {
        TaskUpdateRequest updateRequest(taskBufferOperatingRequest);
        scheduleUpdate(TaskUpdateRequest::Serialize(updateRequest),"/v1/task/updateTask");
    }

    string createInterTaskMission(shared_ptr<InterTaskMissionDescriptor> interTaskMissionDescriptor)
    {
        return scheduleUpdateAndGetResult(InterTaskMissionDescriptor::Serialize(*interTaskMissionDescriptor),"/v1/task/createInterTaskMission");
    }

    void scheduleUpdate(string updateString,string path)
    {
        if(this->started) {
            sendUpdate(this->httpRequestLocation, path,updateString);
        }
    }

    string scheduleUpdateAndGetResult(string updateString,string path)
    {
        if(this->started) {
            return sendUpdateAndGetResult(this->httpRequestLocation, path,updateString);
        }
        return InterTaskDataHandle::Serialize(InterTaskDataHandle(false,"Task "+this->taskId->ToString()+" is not started!"));
    }

    void sendUpdate(string location,string path,string updateString)
    {
        spdlog::debug("Schedule string is :"+location+"|"+path+"|");
        string linkString = location+path;
        if(updateString != "")
            restfulClient->POST_GetResult(location,linkString,{TaskId::Serialize(*(this->taskId)),updateString});
        else
            restfulClient->POST_GetResult(location,linkString,{TaskId::Serialize(*(this->taskId))});
    }

    string sendUpdateAndGetResult(string location,string path,string updateString)
    {
        spdlog::debug("Schedule string is :"+location+"|"+path+"|");
        string linkString = location+path;
        if(updateString != "")
            return restfulClient->POST_GetResult(location,linkString,{TaskId::Serialize(*(this->taskId)),updateString});
        else
            return restfulClient->POST_GetResult(location,linkString,{TaskId::Serialize(*(this->taskId))});
    }

    shared_ptr<TaskInfo> getTaskInfo()
    {
        return this->taskInfoFetcher->getTaskInfo();
    }

    bool isTaskDependenciesSatisfied()
    {
        return this->taskInfoFetcher->isDependenciesSatisfied();
    }


    bool isTaskOutputBufferExpandTrend()
    {
        return this->taskInfoFetcher->isTaskOutputBufferExpandTrend();
    }
    bool isTaskOutputBufferRestTrend()
    {
        return this->taskInfoFetcher->isTaskOutputBufferRestTrend();
    }
    bool isTaskOutputBufferShrinkTrend()
    {
        return this->taskInfoFetcher->isTaskOutputBufferShrinkTrend();
    }
    bool isTaskExchangeBufferExpandTrend()
    {
        return this->taskInfoFetcher->isTaskExchangeBufferExpandTrend();
    }
    bool isTaskExchangeBufferRestTrend()
    {
        return this->taskInfoFetcher->isTaskExchangeBufferRestTrend();
    }
    bool isTaskExchangeBufferShrinkTrend()
    {
        return this->taskInfoFetcher->isTaskExchangeBufferShrinkTrend();
    }



    void close()
    {
        scheduleUpdate("","/v1/task/closeTask");
        this->restfulClient = NULL;
    }


    void abort()
    {
        scheduleUpdate("","/v1/task/abortTask");
    }
};
#endif //OLVP_REMOTETASK_HPP
