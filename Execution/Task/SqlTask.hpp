//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_SQLTASK_HPP
#define OLVP_SQLTASK_HPP

#include "SqlTaskExecutionFactory.hpp"
#include "Id/TaskId.hpp"
#include "TaskInfo.hpp"
#include "../Buffer/OutputBufferSchema.hpp"
#include "TaskSource.hpp"

#include "../../Descriptor/TaskInterfere/TaskInterfereRequest.hpp"
#include "../../Descriptor/TaskInterfere/TaskIntraParaUpdateRequest.hpp"
#include "../../Descriptor/TaskInterfere/TaskBufferOperatingRequest.hpp"


using namespace std;

class TaskHolder
{

    std::shared_ptr<SqlTaskExecution> taskExecution = NULL;
    std::shared_ptr<TaskInfo> taskInfo;

public:
    TaskHolder(std::shared_ptr<SqlTaskExecution> taskExecution)
    {
        this->taskExecution = taskExecution;

    }
    std::shared_ptr<SqlTaskExecution> getTaskExecution()
    {
        return this->taskExecution;
    }
    std::shared_ptr<TaskInfo> getTaskInfo()
    {
        return this->taskInfo;
    }
};


class SqlTask
{

    shared_ptr<TaskId> taskId = NULL;
    shared_ptr<PlanFragment> fragment = NULL;
    shared_ptr<SqlTaskExecutionFactory> sqlTaskExecutionFactory = NULL;
    shared_ptr<TaskStateMachine> stateMachine = NULL;
    shared_ptr<OutputBuffer> buffer = NULL;


    shared_ptr<TaskHolder> taskHolder = NULL;
    shared_ptr<Session> session;

    atomic<int> bufferUsing = 0;



public:

    SqlTask(shared_ptr<Session> session,shared_ptr<TaskId> taskId,shared_ptr<SqlTaskExecutionFactory> sqlTaskExecutionFactory)
    {
        this->taskId = taskId;
        this->sqlTaskExecutionFactory = sqlTaskExecutionFactory;
        this->stateMachine = std::make_shared<TaskStateMachine>();

        this->buffer = make_shared<LazyOutputBuffer>(taskId);
        this->session = session;


    }

    void updateOrCreateTask(shared_ptr<PlanFragment> fragment,  shared_ptr<OutputBufferSchema> schema ,shared_ptr<TaskSource> taskSource,
                                shared_ptr<TaskInterfereRequest> taskInterfereRequest) {


        if(this->taskHolder == NULL)
        {
            if(fragment == NULL)
            {
                spdlog::critical("Task is not running,but fragment is NULL!");
                return;
            }
              this->taskHolder = make_shared<TaskHolder>(
                      this->sqlTaskExecutionFactory->createRuntimeMachine(
                              this->session,
                              this->taskId,
                              this->stateMachine,
                              fragment,
                              this->buffer));
        }
        else if(this->taskHolder->getTaskExecution() == NULL)
        {
            if(fragment == NULL)
            {
                spdlog::critical("Task is not running,but fragment is NULL!");
                return;
            }
            this->taskHolder = make_shared<TaskHolder>(
                    this->sqlTaskExecutionFactory->createRuntimeMachine(
                            this->session,
                            this->taskId,
                            this->stateMachine,
                            fragment,
                            this->buffer));
        }



        if(schema != NULL) {

            if(this->buffer == NULL)
                return;

            this->buffer->setOutputBuffersSchema(*schema);
            this->buffer->addTaskContext(this->taskHolder->getTaskExecution()->getTaskContext());
        }
        if(this->stateMachine->isFinished())
            return;

        if(taskSource != NULL)
        if(!taskSource->getSplits().empty()) {
           this->taskHolder->getTaskExecution()->updateSources(taskSource);
        }

        if(taskInterfereRequest != NULL)
        {
            if(taskInterfereRequest->getType() == "TaskIntraParaUpdateRequest")
            {
                spdlog::debug("sqlTask TaskIntraParaUpdateRequest in");
                this->updateTaskIntraParallelism(static_pointer_cast<TaskIntraParaUpdateRequest>(taskInterfereRequest));
                spdlog::debug("sqlTask TaskIntraParaUpdateRequest out");
            }
            else if(taskInterfereRequest->getType() == "TaskBufferOperatingRequest")
            {
                this->operateTaskBuffer(static_pointer_cast<TaskBufferOperatingRequest>(taskInterfereRequest));
            }

        }


    }

    void operateTaskBuffer(shared_ptr<TaskBufferOperatingRequest> taskBufferOperating)
    {
        if(this->stateMachine->isFinished())
            return;
        if(this->buffer == NULL)
            return;
        vector<string> bufferIds = taskBufferOperating->getBufferIds();
        if(taskBufferOperating->getType() == TaskBufferOperatingRequest::CLOSE_BUFFER) {
            for (auto bufferId: bufferIds) {
                this->buffer->closeBuffer(bufferId);
            }
        }
        else if(taskBufferOperating->getType() == TaskBufferOperatingRequest::CLOSE_BUFFER_GROUP) {
            for (auto bufferId: bufferIds) {
                this->buffer->closeBufferGroup(bufferId);
            }
        }
        else
        {
            spdlog::critical("Unknown Task Buffer OpType!");
        }
    }


    void updateTaskIntraParallelism(shared_ptr<TaskIntraParaUpdateRequest> taskIntraParaUpdateRequest)
    {
        if(this->stateMachine->isFinished())
            return;
        else if(!this->stateMachine->isRunning())
        {
            thread(updateTaskIntraParallelismWaiter,this,taskIntraParaUpdateRequest).detach();
            return;
        }

        spdlog::debug("updateTaskIntraParallelism in");
        string pipelineId = taskIntraParaUpdateRequest->getPipelineId();

        if(taskIntraParaUpdateRequest->getUpdateType() == "incre") {
            if (pipelineId != "-1") {
                for (int i = 0; i < atoi(taskIntraParaUpdateRequest->getUpdateParaCount().c_str()); i++)
                    this->taskHolder->getTaskExecution()->increasePipelineDriver(PipelineId(pipelineId));
            } else {
                string updateCount = taskIntraParaUpdateRequest->getUpdateParaCount();
                this->taskHolder->getTaskExecution()->increaseAllScalablePipelineForTask(atoi(updateCount.c_str()));
            }
        }
        else if(taskIntraParaUpdateRequest->getUpdateType() == "decre") {
            if (pipelineId != "-1") {
                for (int i = 0; i < atoi(taskIntraParaUpdateRequest->getUpdateParaCount().c_str()); i++)
                    this->taskHolder->getTaskExecution()->closePipelineDriver(PipelineId(pipelineId));
            }
        }
        spdlog::debug("updateTaskIntraParallelism out");
    }

    static void updateTaskIntraParallelismWaiter(SqlTask* sqlTask,shared_ptr<TaskIntraParaUpdateRequest> taskIntraParaUpdateRequest)
    {
        while(true)
        {
            if(sqlTask->stateMachine->isRunning())
            {
                break;
            }
            if(sqlTask->stateMachine->isFinished())
                return;
        }
        spdlog::debug("updateTaskIntraParallelism in");
        string pipelineId = taskIntraParaUpdateRequest->getPipelineId();

        if(pipelineId != "-1") {
            for (int i = 0; i < atoi(taskIntraParaUpdateRequest->getUpdateParaCount().c_str()); i++)
                sqlTask->taskHolder->getTaskExecution()->increaseASourceCPUPipeLine(PipelineId(pipelineId));
        }
        else
        {
            string updateCount = taskIntraParaUpdateRequest->getUpdateParaCount();
            sqlTask->taskHolder->getTaskExecution()->increaseAllScalablePipelineForTask(atoi(updateCount.c_str()));
        }
        spdlog::debug("updateTaskIntraParallelism out");
    }

    TaskInfo getTaskInfo()
    {
        string taskIdString;
        string state;
        shared_ptr<TaskInfoDescriptor> taskDesc;

        if(this->stateMachine->getState() == TaskStateMachine::PLANNED) {
            taskIdString = this->taskId->ToString();
            state = "PLANNING";

            vector<shared_ptr<PipelineDescriptor>> emptyDescs;
            taskDesc = make_shared<TaskInfoDescriptor>(taskIdString,state,emptyDescs,this->taskHolder->getTaskExecution()->getTaskContext());
        }
        else if(this->stateMachine->getState() == TaskStateMachine::RUNNING) {
            taskIdString = this->taskId->ToString();
            state = "RUNNING";
            taskDesc = make_shared<TaskInfoDescriptor>(taskIdString,state,this->taskHolder->getTaskExecution()->getPipelineDescriptors(),this->taskHolder->getTaskExecution()->getTaskContext());
        }
        else if(this->stateMachine->getState() == TaskStateMachine::FINISHED) {
            taskIdString = this->taskId->ToString();
            state = "FINISHED";
            taskDesc = make_shared<TaskInfoDescriptor>(taskIdString,state,this->taskHolder->getTaskExecution()->getPipelineDescriptors(),this->taskHolder->getTaskExecution()->getTaskContext());
        }
        else {
            taskIdString = this->taskId->ToString();
            state = "UNKNOWN";
            vector<shared_ptr<PipelineDescriptor>> emptyDescs;
            taskDesc = make_shared<TaskInfoDescriptor>(taskIdString,state,emptyDescs,this->taskHolder->getTaskExecution()->getTaskContext());
        }

        return taskDesc;

    }

    vector<shared_ptr<DataPage>> getTaskResults(string bufferId,string token,int size) {

        if(this->buffer == NULL)
            return {DataPage::getEndPage()};

        this->bufferUsing++;
        vector<shared_ptr<DataPage>> re = this->buffer->getPages(bufferId,atol(token.c_str()),size);
        this->bufferUsing--;

        return re;
    }

    void triggerTaskBufferNoteEvent(string taskId,string bufferId,string note) {

        if(this->buffer == NULL)
            return;

        this->bufferUsing++;
        this->buffer->triggerNoteEvent(taskId,bufferId,note);
        this->bufferUsing--;
    
    }


    shared_ptr<TaskStateMachine> getStateMachine()
    {
        return this->stateMachine;
    }



    void close()
    {
        if(bufferUsing == 0)
            this->buffer = NULL;
        else
        {
            int temp;
            std::thread([](SqlTask *s){
                while(s->bufferUsing > 0);
                s->buffer = NULL;
            }, this).detach();
        }
    }







};

#endif //OLVP_SQLTASK_HPP
