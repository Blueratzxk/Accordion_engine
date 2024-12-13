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
//#include "../../Descriptor/TaskBufferOpRequest.hpp"
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


public:
    HttpRemoteTask(shared_ptr<Event> eventListener,shared_ptr<TaskId> taskId, shared_ptr<PlanFragment> fragment,string nodeLocation,
                   shared_ptr<OutputBufferSchema> schema,shared_ptr<TaskSource> initial_taskSources,shared_ptr<Session> session){
        this->taskId = taskId;
        this->httpRequestLocation = nodeLocation;
        this->schema = schema;
        this->initial_taskSource = initial_taskSources;
        this->eventListener = eventListener;
        this->fragment = fragment;
        this->session = session;
        this->taskInfoFetcher = make_shared<TaskInfoFetcher>(this->taskId,this->httpRequestLocation,this->eventListener);
        this->restfulClient = make_shared<RestfulClient>();
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
        TaskUpdateRequest request(this->initial_taskSource,this->schema,this->fragment,NULL,this->session->toSessionRepresentation());
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
        TaskUpdateRequest updateRequest(this->schema);
        scheduleUpdate(TaskUpdateRequest::Serialize(updateRequest),"/v1/task/updateTask");
    }
    void operateOutputBuffer(shared_ptr<TaskBufferOperatingRequest> taskBufferOperatingRequest)
    {
        TaskUpdateRequest updateRequest(taskBufferOperatingRequest);
        scheduleUpdate(TaskUpdateRequest::Serialize(updateRequest),"/v1/task/updateTask");
    }

    void scheduleUpdate(string updateString,string path)
    {
        if(this->started) {
            sendUpdate(this->httpRequestLocation, path,updateString);
        }
    }

    void sendUpdate(string location,string path,string updateString)
    {
        spdlog::debug("Schedule string is :"+location+"|"+path+"|");
        string linkString = location+path;
        if(updateString != "")
            this->restfulClient->POST(linkString,{TaskId::Serialize(*(this->taskId)),updateString});
        else
            this->restfulClient->POST(linkString,{TaskId::Serialize(*(this->taskId))});
    }

    shared_ptr<TaskInfo> getTaskInfo()
    {
        return this->taskInfoFetcher->getTaskInfo();
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

    }
};
#endif //OLVP_REMOTETASK_HPP
