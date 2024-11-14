//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_TASKMANAGER_HPP
#define OLVP_TASKMANAGER_HPP


//#include "../../common.h"
#include "TaskExecutor/TaskExecutor.hpp"
#include "SqlTaskExecutionFactory.hpp"
#include "TaskSource.hpp"
#include "../../Execution/Buffer/OutputBufferSchema.hpp"
#include "SqlTask.hpp"

#include "Context/QueryContext.h"
#include "Context/TaskContext.h"

#include "tbb/concurrent_map.h"
#include "../../Descriptor/TaskInterfere/TaskInterfereRequest.hpp"
#include "../../Descriptor/TaskUpdateRequest.hpp"


class TaskManager
{

    shared_ptr<TaskExecutor> taskExecutor = NULL;
    shared_ptr<LocalExecutionPlanner> planner = NULL;
    map<string,shared_ptr<SqlTask>> tasks;
    mutex taskMapLock;

    shared_ptr<QueryContext> queryContext;
    shared_ptr<TasksRuntimeStats> tasksRuntimeStats;

public:

    TaskManager()
    {
        this->taskExecutor = make_shared<TaskExecutor>();
        this->taskExecutor->start();
        this->planner = make_shared<LocalExecutionPlanner>();
        this->tasksRuntimeStats = make_shared<TasksRuntimeStats>();
        this->queryContext = make_shared<QueryContext>();


    }



    map<string,set<shared_ptr<TaskContext>>> getAllRunningTaskContexts()
    {
        map<string,set<shared_ptr<TaskContext>>> tcs;
        taskMapLock.lock();
        tcs= this->queryContext->getAllRunningTaskContexts();
        taskMapLock.unlock();
        return tcs;
    }

    shared_ptr<SqlTask> checkTask(TaskId taskId,shared_ptr<SessionRepresentation> sessionRepresentation)
    {
        shared_ptr<SqlTask> sqlTaskPtr = NULL;
        taskMapLock.lock();
        try {
            if (tasks.find(taskId.ToString()) == tasks.end()) {

                multimap<string,vector<string>> runtimeConfigs;
                if(sessionRepresentation != NULL) {
                    runtimeConfigs = sessionRepresentation->getRuntimeConfigs();
                    for (auto con: runtimeConfigs) {
                        cout << "hehe " << con.first << "|" << con.second.size() << endl;
                    }
                }

                auto session = make_shared<Session>(taskId.getQueryId().getId(),taskId.getStageExecutionId().getId());

                session->setRumtimeConfigs(runtimeConfigs);

                auto factory = make_shared<SqlTaskExecutionFactory>(this->tasksRuntimeStats,this->queryContext, this->planner,
                                                                    this->taskExecutor);
                auto newTaskId = make_shared<TaskId>(taskId.getQueryId().getId(), taskId.getStageExecutionId().getId(),
                                                     taskId.getStageId().getId(), taskId.getId());



                auto sqlTask = make_shared<SqlTask>(session,newTaskId, factory);

                tasks[taskId.ToString()] = sqlTask;
                sqlTaskPtr = sqlTask;

                this->tasksRuntimeStats->increaseTaskNums();
            } else {
                sqlTaskPtr = tasks[taskId.ToString()];
            }
        }
        catch (exception &e)
        {
            spdlog::critical(e.what());
        }
        taskMapLock.unlock();
        return sqlTaskPtr;
    }




    shared_ptr<SqlTask> findTask(TaskId taskId)
    {
        shared_ptr<SqlTask> sqlTaskPtr = NULL;
        taskMapLock.lock();
        if (tasks.find(taskId.ToString()) == tasks.end()) {
            sqlTaskPtr = NULL;
        }
        else
            sqlTaskPtr = tasks[taskId.ToString()];
        taskMapLock.unlock();
        return sqlTaskPtr;
    }


    shared_ptr<TaskInfo> updateTask(TaskId taskId,shared_ptr<TaskUpdateRequest> taskUpdateRequest)
    {

        shared_ptr<PlanFragment> fragment = taskUpdateRequest->getFragment();
        shared_ptr<TaskSource> sources = taskUpdateRequest->getTaskSource();
        shared_ptr<OutputBufferSchema> schema = taskUpdateRequest->getSchema();
        shared_ptr<TaskInterfereRequest> taskInterfereRequest = taskUpdateRequest->getTaskInterfereRequest();
        shared_ptr<SessionRepresentation> sessionRepresentation = taskUpdateRequest->getSessionRepresentation();

        shared_ptr<SqlTask> sqlTaskPtr = this->checkTask(taskId,sessionRepresentation);

        sqlTaskPtr->updateOrCreateTask(fragment,schema,sources,taskInterfereRequest);

        spdlog::debug("Task manager updateTask in");
        vector<shared_ptr<PipelineDescriptor>> emptyDescs;
        auto info = make_shared<TaskInfoDescriptor>(taskId.ToString(),"Create Task Ok!");
        auto taskInfo = make_shared<TaskInfo>(info);
        spdlog::debug("Task manager updateTask out");
        return taskInfo;

    }

    vector<shared_ptr<DataPage>> getTaskResults(TaskId taskId, OutputBufferId bufferId, int maxSize)
    {
        shared_ptr<SqlTask> sqlTaskPtr = this->findTask(taskId);
        if(sqlTaskPtr == NULL)
            return {};
        else
            return sqlTaskPtr->getTaskResults(bufferId.get(),"0",maxSize);
    }

    void triggerTaskBufferNoteEvent(TaskId taskId,OutputBufferId bufferId,string note)
    {
        shared_ptr<SqlTask> sqlTaskPtr = this->findTask(taskId);
        if(sqlTaskPtr == NULL)
            return ;
        else
            return sqlTaskPtr->triggerTaskBufferNoteEvent(taskId.ToString(),bufferId.get(),note);
    }

    TaskInfo getTaskInfo(TaskId taskId){


        shared_ptr<SqlTask> sqlTaskPtr = this->findTask(taskId);
        if(sqlTaskPtr == NULL) {

            vector<shared_ptr<PipelineDescriptor>> emptyDescs;
            auto info = make_shared<TaskInfoDescriptor>("No TaskId","Cannot find this Task!");
            return TaskInfo(info);

        }
        else
            return sqlTaskPtr->getTaskInfo();

    }

    vector<TaskInfo> getAllTaskInfo(){


        vector<TaskInfo> taskInfos;
        taskMapLock.lock();
        for(auto task : this->tasks)
        {
            taskInfos.push_back(task.second->getTaskInfo());
        }
        taskMapLock.unlock();
        return taskInfos;

    }
    TaskInfo closeTask(TaskId taskId)
    {
        shared_ptr<SqlTask> sqlTaskPtr = this->findTask(taskId);
        if(sqlTaskPtr == NULL) {

            vector<shared_ptr<PipelineDescriptor>> emptyDescs;
            auto info = make_shared<TaskInfoDescriptor>("No TaskId","Cannot find this Task!");
            return TaskInfo(info);

        }
        else
        {
            this->tasksRuntimeStats->decreaseTaskNums();
            sqlTaskPtr->close();
            return sqlTaskPtr->getTaskInfo();
        }
    }

    int getAllActiveThreadNums() {
        int nums = this->tasksRuntimeStats->getAllActiveThreadNums();
        return nums;
    }
    int getAllActiveTaskNums() {
        int nums = this->tasksRuntimeStats->getAllActiveTasks();
        return nums;
    }


  //  List<TaskInfo> getAllTaskInfo();

  //  TaskInfo getTaskInfo(TaskId taskId);

   // TaskStatus getTaskStatus(TaskId taskId);

  //  ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState);


  //  TaskInfo cancelTask(TaskId taskId);

   // TaskInfo abortTask(TaskId taskId);

   // ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBufferId bufferId, long startingSequenceId, DataSize maxSize);

    //void removeRemoteSource(TaskId taskId, TaskId remoteSourceTaskId);

    //void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener);

 //   void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long sequenceId);



};



#endif //OLVP_TASKMANAGER_HPP
