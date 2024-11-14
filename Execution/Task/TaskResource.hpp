//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_TASKRESOURCE_HPP
#define OLVP_TASKRESOURCE_HPP

#include "TaskManager.hpp"
#include "../../Descriptor/TaskUpdateRequest.hpp"


class TaskResourceManager
{

    std::shared_ptr<TaskManager> taskManager = NULL;

public:

    TaskResourceManager(std::shared_ptr<TaskManager> taskManager){

        this->taskManager = taskManager;
    }


    TaskInfo updateTask(TaskId taskId,std::shared_ptr<TaskUpdateRequest> taskUpdateRequest)
    {
        spdlog::debug("TaskSource updateTask in");
        return *this->taskManager->updateTask(taskId,taskUpdateRequest);
    }

    vector<shared_ptr<DataPage>> getTaskResults(TaskId taskId, OutputBufferId bufferId, int maxSize)
    {
        return this->taskManager->getTaskResults(taskId,bufferId,maxSize);
    }

    void triggerTaskBufferNoteEvent(TaskId taskId, OutputBufferId bufferId,string note)
    {
        this->taskManager->triggerTaskBufferNoteEvent(taskId,bufferId.get(),note);
    }

    TaskInfo getTaskInfo(TaskId taskId)
    {
        return this->taskManager->getTaskInfo(taskId);
    }

    TaskInfo closeTask(TaskId taskId)
    {
        return this->taskManager->closeTask(taskId);
    }

    int getAllActiveThreadNums() {
        int nums = this->taskManager->getAllActiveThreadNums();
        return nums;
    }
    int getAllActiveTaskNums() {
        int nums = this->taskManager->getAllActiveTaskNums();
        return nums;
    }

    vector<TaskInfo> getAllTaskInfo()
    {
        return this->taskManager->getAllTaskInfo();
    }


    /*
    TaskInfo updateTaskTest2(TaskId taskId,PlanNode *pPlanNode,shared_ptr<TaskSource> tss,OutputBufferSchema schema)
    {
        return *this->taskManager->updateTaskTest2(taskId, shared_ptr<PlanNode>(pPlanNode),tss,schema);
    }

     */




};

#endif //OLVP_TASKRESOURCE_HPP
