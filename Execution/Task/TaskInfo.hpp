//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_TASKINFO_HPP
#define OLVP_TASKINFO_HPP

#include <string>
#include "nlohmann/json.hpp"
#include "TaskInfos/TaskInfoDescriptor.hpp"
using namespace std;
class TaskInfo
{
    shared_ptr<TaskInfoDescriptor> taskInfoDescriptor;
    double throughput = 0;

public:
    TaskInfo(shared_ptr<TaskInfoDescriptor> taskInfoDescriptor){
        this->taskInfoDescriptor = taskInfoDescriptor;
    }
    TaskInfo(shared_ptr<TaskInfoDescriptor> taskInfoDescriptor,long throughput){
        this->taskInfoDescriptor = taskInfoDescriptor;
        this->throughput = throughput;
    }

    TaskInfo(){

    }

    shared_ptr<TaskInfoDescriptor> getTaskInfoDescriptor()
    {
        return this->taskInfoDescriptor;
    }

    string getStatus()
    {
        return this->taskInfoDescriptor->getTaskState();
    }

    static string Serialize(TaskInfo taskInfo)
    {
        nlohmann::json json;


        json["taskId"] = taskInfo.taskInfoDescriptor->getTaskId();
        json["status"] = taskInfo.taskInfoDescriptor->getTaskState();
        json["runningTime"] = taskInfo.taskInfoDescriptor->getRunningTime();
        json["joinInfoDescriptor"] = JoinInfoDescriptor::Serialize(taskInfo.taskInfoDescriptor->getJoinInfoDescriptor());

        json["bufferInfoDescriptor"] = BufferInfoDescriptor::Serialize(taskInfo.taskInfoDescriptor->getBufferInfoDescriptor());


        json["taskCpuUsageDescriptor"] = TaskCpuUsageDescriptor::Serialize(taskInfo.getTaskInfoDescriptor()->getTaskCpuUsageDescriptor());

        vector<string> pipelineInfos;
        vector<shared_ptr<PipelineDescriptor>> pInfos = taskInfo.taskInfoDescriptor->getPipelineDescriptors();

        for(auto pInfo : pInfos)
            pipelineInfos.push_back(PipelineDescriptor::Serialize(*pInfo));


        json["pipelineInfos"] = pipelineInfos;
        json["taskThroughputInfo"] = TaskThroughputInfo::Serialize(taskInfo.taskInfoDescriptor->getTaskThroughputInfo());

        json["taskThroughput"] = taskInfo.throughput;



        string result = json.dump();

        return result;
    }
    static shared_ptr<TaskInfo> Deserialize(string taskInfo)
    {
        nlohmann::json json = nlohmann::json::parse(taskInfo);

        vector<string> pipeInfos = json["pipelineInfos"];
        vector<shared_ptr<PipelineDescriptor>> pInfos;

        for(auto pipeInfo : pipeInfos)
        {
            shared_ptr<PipelineDescriptor> pipelineDescriptor = PipelineDescriptor::Deserialize(pipeInfo);
            pInfos.push_back(pipelineDescriptor);
        }

        auto taskInfoDesc = make_shared<TaskInfoDescriptor>(json["taskId"],json["status"],pInfos,json["runningTime"],
                                                            *TaskThroughputInfo::Deserialize(json["taskThroughputInfo"]),
                                                            *JoinInfoDescriptor::Deserialize(json["joinInfoDescriptor"]),
                                                            *BufferInfoDescriptor::Deserialize(json["bufferInfoDescriptor"]));

        taskInfoDesc->addTaskCpuUsageDescriptor(TaskCpuUsageDescriptor::Deserialize(json["taskCpuUsageDescriptor"]));

        return make_shared<TaskInfo>(taskInfoDesc,json["taskThroughput"]);
    }

    static string Visualization(TaskInfo taskInfo){

        string json = taskInfo.taskInfoDescriptor->ToString();
        nlohmann::json  jj = nlohmann::json::parse(json);
        jj["taskThroughput"] = taskInfo.throughput;
        string output = jj.dump();
        return output;

    }


};
#endif //OLVP_TASKINFO_HPP
