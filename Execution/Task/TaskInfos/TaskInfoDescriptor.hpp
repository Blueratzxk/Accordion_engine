//
// Created by zxk on 6/9/23.
//

#ifndef OLVP_TASKINFODESCRIPTOR_HPP
#define OLVP_TASKINFODESCRIPTOR_HPP

#include "PipelineDescriptor.hpp"
#include "../Context/TaskContext.h"
#include "../../../System/TaskServer.h"
#include "../../../Execution/Task/Statistics/CPU/TaskCpuUsageDescriptor.hpp"
#include "../../../Execution/Buffer/BufferInfoDescriptor.hpp"
#include "../../../Execution/Task/TaskInfos/JoinInfoDescriptor.hpp"
class TaskInfoDescriptor
{
    string taskId;
    string taskState;
    double runningTime;
    vector<shared_ptr<PipelineDescriptor>> pipelineDescriptor;
    TaskThroughputInfo taskThroughputInfo;
    TaskCpuUsageDescriptor taskCpuUsageDescriptor;
    BufferInfoDescriptor bufferInfoDescriptor;
    JoinInfoDescriptor joinInfoDescriptor;



public:
    TaskInfoDescriptor(string taskId,string taskState,vector<shared_ptr<PipelineDescriptor>> pipelineDescriptor,double runningTime,
                       TaskThroughputInfo taskThroughputInfo,JoinInfoDescriptor joinInfoDescriptor,BufferInfoDescriptor bufferInfoDescriptor)
    {
        this->taskId = taskId;
        this->taskState = taskState;
        this->pipelineDescriptor = pipelineDescriptor;
        this->runningTime = runningTime;
        this->taskThroughputInfo = taskThroughputInfo;
        this->joinInfoDescriptor = joinInfoDescriptor;

        this->bufferInfoDescriptor = bufferInfoDescriptor;
    }

    TaskInfoDescriptor(string taskId,string taskState,vector<shared_ptr<PipelineDescriptor>> pipelineDescriptor,shared_ptr<TaskContext> taskContext)
    {
        this->taskId = taskId;
        this->taskState = taskState;
        this->pipelineDescriptor = pipelineDescriptor;
        this->runningTime = taskContext->getRunningTime();
        this->taskThroughputInfo = taskContext->getTaskThroughputInfo();

        this->joinInfoDescriptor = taskContext->getJoinInfoDescriptor();

        this->bufferInfoDescriptor = taskContext->getBufferInfoDescriptor();

    }
    TaskInfoDescriptor(string taskId,string taskState)
    {
        this->taskId = taskId;
        this->taskState = taskState;
        this->taskThroughputInfo = TaskThroughputInfo();

    }
    void addTaskCpuUsageDescriptor(TaskCpuUsageDescriptor taskCpuUsageDescriptor)
    {
        this->taskCpuUsageDescriptor = taskCpuUsageDescriptor;
    }

    string getTaskState()
    {
        return this->taskState;
    }
    string getTaskId()
    {
        return this->taskId;
    }
    vector<shared_ptr<PipelineDescriptor>> getPipelineDescriptors()
    {
        return this->pipelineDescriptor;
    }
    double getRunningTime()
    {
        return this->runningTime;
    }


    TaskCpuUsageDescriptor getTaskCpuUsageDescriptor()
    {
        return this->taskCpuUsageDescriptor;
    }
    TaskThroughputInfo getTaskThroughputInfo()
    {
        return this->taskThroughputInfo;
    }
    BufferInfoDescriptor getBufferInfoDescriptor()
    {
        return this->bufferInfoDescriptor;
    }

    JoinInfoDescriptor getJoinInfoDescriptor()
    {
        return this->joinInfoDescriptor;
    }

    string ToString()
    {
        string result;
        result.append("{");
        result.append("\"TaskId\":");
        result.append("\""+this->taskId+"\"");
        result.append(",");
        result.append("\"TaskState\":");
        result.append("\""+this->taskState+"\"");
        result.append(",");
        result.append("\"RunningTime\":");
        result.append("\""+to_string(this->runningTime)+"\"");
        result.append(",");
        result.append("\"JoinNums\":");
        result.append("\""+to_string(this->joinInfoDescriptor.getJoinNums())+"\"");
        result.append(",");
        result.append("\"BuildNums\":");
        result.append("\""+to_string(this->joinInfoDescriptor.getBuildNums())+"\"");
        result.append(",");
        result.append("\"CurrentTupleCount\":");
        result.append("\""+to_string(this->taskThroughputInfo.getCurrentTupleCount())+"\"");
        result.append(",");
        result.append("\"TupleCountCollectedTimeStamp\":");
        result.append("\""+to_string(this->taskThroughputInfo.getTimeStamp())+"\"");
        result.append(",");
        result.append("\"buildTime\":");
        result.append("\""+to_string(this->joinInfoDescriptor.getBuildTime())+"\"");
        result.append(",");
        result.append("\"buildProgress\":");

        double progress;
        if(this->joinInfoDescriptor.getAllBuildCount() > 0)
            progress = ((double)this->joinInfoDescriptor.getAllBuildProgress()/(double)this->joinInfoDescriptor.getAllBuildCount())*100.0;
        else
            progress = 100.0;
        result.append("\""+to_string(progress)+"\"");
        result.append(",");
        result.append("\"buildComputingTime\":");
        result.append("\""+to_string(this->joinInfoDescriptor.getBuildComputingTime())+"\"");
        result.append(",");
        result.append("\"Pipeline Descriptors\":");
        result.append("[");
        for(auto desc : this->pipelineDescriptor)
        {
            result.append(desc->ToString());
            result.append(",");
        }
        if(!this->pipelineDescriptor.empty())
            result.pop_back();
        result.append("]");
        result.append("}");
        return result;
    }

    static string Serialize(TaskInfoDescriptor taskDescriptor)
    {
        nlohmann::json json;

        json["taskId"] = taskDescriptor.taskId;
        json["taskState"] = taskDescriptor.taskState;
        json["runningTime"] = taskDescriptor.runningTime;

        vector<string> pipelineDescs;
        for(auto pipelineDesc : taskDescriptor.pipelineDescriptor)
        {
            pipelineDescs.push_back( PipelineDescriptor::Serialize(*pipelineDesc));
        }
        json["pipelineDescriptor"] = pipelineDescs;
        json["taskThroughputInfo"] = TaskThroughputInfo::Serialize(taskDescriptor.taskThroughputInfo);

        json["joinInfoDescriptor"] = JoinInfoDescriptor::Serialize(taskDescriptor.joinInfoDescriptor);


        json["taskCpuUsageDescriptor"] = TaskCpuUsageDescriptor::Serialize(taskDescriptor.taskCpuUsageDescriptor);


        json["bufferInfoDescriptor"] = BufferInfoDescriptor::Serialize(taskDescriptor.bufferInfoDescriptor);

        string result = json.dump();
        return result;
    }

    static shared_ptr<TaskInfoDescriptor> Deserialize(string taskInfoDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(taskInfoDescriptor);
        vector<shared_ptr<PipelineDescriptor>> pipelineDescriptors;
        vector<string> pipelineStrs;
        pipelineStrs = json["pipelineTemplate"];

        for(auto pipelineStr : pipelineStrs)
        {
            pipelineDescriptors.push_back(PipelineDescriptor::Deserialize(pipelineStr));
        }
        TaskThroughputInfo ttinfo = *TaskThroughputInfo::Deserialize(json["taskThroughputInfo"]);

       auto taskCpuUsageDescriptor = TaskCpuUsageDescriptor::Deserialize(json["taskCpuUsageDescriptor"]);

       auto result = make_shared<TaskInfoDescriptor>(json["taskId"],json["taskState"],pipelineDescriptors,json["runningTime"],
                                                     ttinfo, *JoinInfoDescriptor::Deserialize(json["joinInfoDescriptor"]),
                                                     *BufferInfoDescriptor::Deserialize(json["bufferInfoDescriptor"]));
       result->addTaskCpuUsageDescriptor(taskCpuUsageDescriptor);
       return  result;
    }

};


#endif //OLVP_TASKINFODESCRIPTOR_HPP
