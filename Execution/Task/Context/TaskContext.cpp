//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_TASKCONTEXT_CPP
#define OLVP_TASKCONTEXT_CPP

#include "PipelineContext.h"
#include "TaskContext.h"
#include "QueryContext.h"


TaskContext::TaskContext(shared_ptr<TasksRuntimeStats> tasksRuntimeStats,weak_ptr<QueryContext> queryContext,shared_ptr<TaskId> taskId,shared_ptr<TaskStateMachine> stateMachine,shared_ptr<OutputBuffer> outputBuffer){
    this->stateMachine = stateMachine;
    this->queryContext = queryContext;
    this->taskId = taskId;
    this->outputBuffer = outputBuffer;
    this->tasksRuntimeStats = tasksRuntimeStats;

}

shared_ptr<PipelineContext> TaskContext::addPipelineContext(PipelineId pipelineId){

    this->lock.lock();
    shared_ptr<PipelineContext> pipelineContext =  make_shared<PipelineContext>(shared_from_this(),pipelineId);
    this->pipelineContexts[pipelineId] = pipelineContext;
    this->activePipelineContexts.insert(pipelineContext);
    this->lock.unlock();
    return pipelineContext;
}

shared_ptr<PipelineContext> TaskContext::getPipelineContext(PipelineId pipelineId) {
    this->lock.lock();
    if(!(this->pipelineContexts.find(pipelineId) == this->pipelineContexts.end())){
        shared_ptr<PipelineContext> pipelineContext = this->pipelineContexts[pipelineId];
        this->lock.unlock();
        return pipelineContext;
    }

    this->lock.unlock();
    spdlog::critical("ERROR! Cannot find the pipelineContext!");
    return NULL;

}

void TaskContext::finishPipelineContext(PipelineId pipelineId) {

    this->lock.lock();
    if(!(this->pipelineContexts.find(pipelineId) == this->pipelineContexts.end())){
        shared_ptr<PipelineContext> pipelineContext = this->pipelineContexts[pipelineId];
        this->activePipelineContexts.erase(pipelineContext);

        spdlog::debug("PipelineContext "+pipelineId.get()+" finished!");
        if(this->activePipelineContexts.empty()){
            spdlog::info("Task ["+this->taskId->ToString()+"] Finished!");
            this->stateMachine->finished();
        }
    }
    else
        spdlog::critical("ERROR! Cannot find the pipelineContext!");

    this->lock.unlock();
}


map<PipelineId,shared_ptr<PipelineContext>> TaskContext::getPipelineContexts() {
    return this->pipelineContexts;
}

shared_ptr<TaskStateMachine> TaskContext::getState() {
    return this->stateMachine;
}

shared_ptr<OutputBuffer> TaskContext::getOutputBuffer() {
    return this->outputBuffer;
}

TaskThroughputInfo TaskContext::getTaskThroughputInfo() {
    return TaskThroughputInfo(this->currentTotalTupleCount,this->currentTotalTupleCount*this->outputTupleByteWidth,
                              this->getRemainingBufferTupleCount(),this->getRemainingTableTupleCount(),
                              this->lastEnqueuedTupleCount,this->totalInputBytes);
}

JoinInfoDescriptor TaskContext::getJoinInfoDescriptor() {
    return JoinInfoDescriptor(this->joinNum,this->buildNums,this->getBuildTime(),this->getBuildComputingTime());
}

void TaskContext::addTotalTupleCount(long increment) {
    this->currentTotalTupleCount += increment;
}

long TaskContext::getRemainingTableTupleCount(){
    return this->remainingTableTupleCount;
}
long TaskContext::getRemainingBufferTupleCount(){
    return this->bufferRemainingTupleCount;
}
void TaskContext::setRemainingTableTupleCount(long count)
{
    this->remainingTableTupleCount = count;
}

shared_ptr<RuntimeConfigParser> TaskContext::getRuntimeConfigs()
{
    return this->queryContext.lock()->getRuntimeConfigs();
}

double TaskContext::getRunningTime() {

    if(!this->stateMachine->isFinished() && this->stateMachine->getState() == TaskStateMachine::RUNNING)
    {
        auto now = std::chrono::steady_clock::now();
        auto startTime = this->stateMachine->getStartTime();

        return std::chrono::duration<double, std::milli>(now - startTime).count();
    }
    else if(this->stateMachine->isFinished())
    {
        return std::chrono::duration<double, std::milli>(this->stateMachine->getEndTime() - this->stateMachine->getStartTime()).count();
    }
    else
        return 0;
}
void TaskContext::reportBuildStartTime(std::chrono::system_clock::time_point time){

    lock.lock();
    if(this->firstStartBuildTime == NULL)
        this->firstStartBuildTime = make_shared<std::chrono::system_clock::time_point>(time);
    else if(time < *(this->firstStartBuildTime))
        *(this->firstStartBuildTime) = time;
    lock.unlock();
};

void TaskContext::reportBuildFinishedTime(std::chrono::system_clock::time_point time){

    lock.lock();
    if(this->lastBuildFinishedTime == NULL)
        this->lastBuildFinishedTime = make_shared<std::chrono::system_clock::time_point>(time);
    else if(time > *(this->lastBuildFinishedTime))
        *(this->lastBuildFinishedTime) = time;
    lock.unlock();

};

void TaskContext::reportBuildTime(double time)
{
    if(time > maxBuildTime)
        maxBuildTime = time;
}

void TaskContext::reportBuildComputingTime(double time)
{
    if(time > maxBuildComputingTime)
        maxBuildComputingTime = time;
}

map<string,set<__pid_t>> TaskContext::getAllTaskTids() {



    set<__pid_t> shuffleTids;
    set<__pid_t> driverTids;

    map<string,set<__pid_t>> allTids;

    this->tidLock.lock();

    for (auto id: this->shuffleExecutorTids)
        shuffleTids.insert(id);

    for (auto pipeline: this->pipelineContexts) {
        auto pipelineTids = pipeline.second->getPipelineTids();
        for (auto id: pipelineTids)
            driverTids.insert(id);
    }
    this->tidLock.unlock();

    allTids["shuffle"] = shuffleTids;
    allTids["driver"] = driverTids;

    return allTids;

}

void TaskContext::addTids(set<__pid_t> ids) {
    this->tidLock.lock();
    for (auto id: ids)
        this->shuffleExecutorTids.insert(id);
    this->tidLock.unlock();
}
void TaskContext::removeTids(set<__pid_t> ids) {
    this->tidLock.lock();
    for (auto id: ids)
        this->shuffleExecutorTids.erase(id);
    this->tidLock.unlock();
}

#endif //OLVP_TASKCONTEXT_CPP
