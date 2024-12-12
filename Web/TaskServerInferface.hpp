//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_TASKSERVERINFERFACE_HPP
#define OLVP_TASKSERVERINFERFACE_HPP




class TaskServerInterFace
{

public:
    TaskServerInterFace(){
    }

    static TaskInfo createOrUpdateTask(string taskId,string request)
    {

        spdlog::debug(taskId+"---"+request);
        return  TaskServer::taskResourceManager->updateTask(*TaskId::Deserialize(taskId),TaskUpdateRequest::Deserialize(request));
    }


    static vector<shared_ptr<DataPage>> getTaskResults(string taskId,string bufferId,int size)
    {
        TaskId id;
        return  TaskServer::taskResourceManager->getTaskResults(*id.StringToObject(taskId),OutputBufferId(bufferId),size);
    }
    static void triggerTaskBufferNoteEvent(string taskId,string bufferId,string note)
    {
        TaskId id;
        return  TaskServer::taskResourceManager->triggerTaskBufferNoteEvent(*id.StringToObject(taskId),OutputBufferId(bufferId),note);
    }


    static TaskInfo closeTask(string taskId)
    {
        return  TaskServer::taskResourceManager->closeTask(*TaskId::Deserialize(taskId));
    }

    static TaskInfo getTaskInfo(string taskId)
    {
        TaskId id;
        shared_ptr<TaskId> tid = id.StringToObject(taskId);
        if(tid == NULL) {
            vector<shared_ptr<PipelineDescriptor>> emptyDescs;
            auto info = make_shared<TaskInfoDescriptor>("No TaskId","TaskId format error!");
            return TaskInfo(info);
        }
        auto cpuUsage = TaskServer::getCpuInfoCollector()->getTasksCpuInfos()->getCpuUsageByTaskId((*tid).ToString());
        auto taskInfo = TaskServer::taskResourceManager->getTaskInfo(*id.StringToObject(taskId));
        taskInfo.getTaskInfoDescriptor()->addTaskCpuUsageDescriptor(cpuUsage);
        return  taskInfo;
    }

    static int getAllActiveTaskNums()
    {
        return TaskServer::taskResourceManager->getAllActiveTaskNums();
    }
    static int getAllActiveThreadNums()
    {
        return TaskServer::taskResourceManager->getAllActiveThreadNums();
    }

    static vector<TaskInfo> getAllTaskInfo()
    {
        return  TaskServer::taskResourceManager->getAllTaskInfo();
    }


    /*
    static TaskInfo createOrUpdateTaskTest2(string taskId,string fragment,string TaskSource,string OutputBufferSchema)
    {
        PlanNodeTreeDeserializer deserializer;
        return  TaskServer::taskResourceManager->updateTaskTest2(*TaskId::Deserialize(taskId),deserializer.Deserialize(fragment),TaskSource::Deserialize(TaskSource),
                                                                 OutputBufferSchema::Deserialize(OutputBufferSchema));
    }

     */
};



#endif //OLVP_TASKSERVERINFERFACE_HPP
