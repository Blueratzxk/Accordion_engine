//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_PIPELINECONTEXT_CPP
#define OLVP_PIPELINECONTEXT_CPP

#include "PipelineContext.h"
#include "DriverContext.h"
#include "TaskContext.h"
#include "spdlog/spdlog.h"



PipelineContext::PipelineContext(weak_ptr<TaskContext> taskContext,PipelineId pipelineId){
    this->taskContext = taskContext;
    this->pipelineId = pipelineId;
}


shared_ptr<DriverContext> PipelineContext::addDriverContext()
{
    lock.lock();
    shared_ptr<DriverContext> driverContext = make_shared<DriverContext>(shared_from_this());
    this->runningContexts.insert(driverContext);
    this->driverContexts.push_back(driverContext);
    this->increaseThreadNums();
    lock.unlock();
    return driverContext;
}

list<shared_ptr<DriverContext>> PipelineContext::getDriverContexts()
{
    lock.lock();
    list<shared_ptr<DriverContext>> driverContextsSnap = this->driverContexts;
    lock.unlock();
    return this->driverContexts;
}


void PipelineContext::addExchangeBufferTurnUpCounter(){
    this->taskContext.lock()->addExchangeBufferSizeTurnUpCounter();
}
void PipelineContext::addExchangeBufferTurnDownCounter(){
    this->taskContext.lock()->addExchangeBufferSizeTurnDownCounter();
}

int PipelineContext::getRunningDriverCount()
{
    lock.lock();
    int count = this->runningContexts.size();
    lock.unlock();
    return count;
}



weak_ptr<TaskContext> PipelineContext::getTaskContext() {
    return this->taskContext;
}

PipelineId PipelineContext::getPipelineId() {
    return this->pipelineId;
}

void PipelineContext::finishDriverContext(shared_ptr<DriverContext> driverContextIn) {

    lock.lock();
    for(auto driverContext : this->runningContexts)
    {
        if(driverContext == driverContextIn)
        {
            this->runningContexts.erase(driverContext);
            this->decreaseThreadNums();
            spdlog::debug("DriverContext finished!");
            break;
        }
    }

    if(this->runningContexts.size() == 0)
    {
        this->taskContext.lock()->finishPipelineContext(this->pipelineId);
    }

    lock.unlock();

}

shared_ptr<OutputBuffer> PipelineContext::getOutputBuffer() {
    return this->taskContext.lock()->getOutputBuffer();
}


void PipelineContext::setRemainingTableTupleCount(long count)
{
    this->taskContext.lock()->setRemainingTableTupleCount(count);
}

shared_ptr<RuntimeConfigParser> PipelineContext::getRuntimeConfigs()
{
    return this->taskContext.lock()->getRuntimeConfigs();
}

void PipelineContext::increaseThreadNums(){
    this->taskContext.lock()->increaseThreadNums();
}
void PipelineContext::decreaseThreadNums(){
    this->taskContext.lock()->decreaseThreadNums();
}

atomic<long> &PipelineContext::getAllBuildCount(){

    return this->taskContext.lock()->getAllBuildCount();
}
atomic<long> &PipelineContext::getAllBuildProgress(){
    return this->taskContext.lock()->getAllBuildProgress();
}



void PipelineContext::reportBuildComplete(){
    taskContext.lock()->reportBuildComplete();
}

bool PipelineContext::hasBuildTask() {
    return taskContext.lock()->hasBuildTask();
}

bool PipelineContext::isAllBuildCompeletedInTask()
{
    return taskContext.lock()->isAllBuildCompeletedInTask();
}

set<shared_ptr<Split>> PipelineContext::getAllRegRemoteSplit(){
    splitsLock.lock();
    auto result = this->AllRemoteSplits;
    splitsLock.unlock();

    return result;
}

int PipelineContext::getAllRegRemoteSplitCount(){
    splitsLock.lock();
    auto result = this->AllRemoteSplits.size();
    splitsLock.unlock();

    return result;
}

void PipelineContext::regRemoteSplit(set<shared_ptr<Split>> splits)
{
    splitsLock.lock();

    if(AllRemoteSplits.empty())
    {
        for(auto splitInsert : splits) {
            AllRemoteSplits.insert(splitInsert);
        }
        splitsLock.unlock();
        return;
    }



    for(auto splitInsert : splits) {

        TaskId taskIdInsert = *(static_pointer_cast<RemoteSplit>(splitInsert->getConnectorSplit())->getTaskId());
        Location locationInsert = *(static_pointer_cast<RemoteSplit>(splitInsert->getConnectorSplit())->getLocation());


        for (auto splitCompare: AllRemoteSplits) {

            auto taskIdCom = *(static_pointer_cast<RemoteSplit>(splitCompare->getConnectorSplit())->getTaskId());
            auto locationCom = *(static_pointer_cast<RemoteSplit>(
                    splitCompare->getConnectorSplit())->getLocation());

            if (!(taskIdInsert == taskIdCom) && !(locationInsert == locationCom)) {
                AllRemoteSplits.insert(splitInsert);
            }
        }

    }
    splitsLock.unlock();
}
void PipelineContext::reportBuildStartTime(std::chrono::system_clock::time_point time){
    this->taskContext.lock()->reportBuildStartTime(time);
}

void PipelineContext::reportBuildFinishedTime(std::chrono::system_clock::time_point time){
    this->taskContext.lock()->reportBuildFinishedTime(time);
}
void PipelineContext::reportBuildTime(double time){
    this->taskContext.lock()->reportBuildTime(time);
}

void PipelineContext::reportBuildComputingTime(string joinId,double time){
    this->taskContext.lock()->reportBuildComputingTime(joinId,time);
};

#endif //OLVP_PIPELINECONTEXT_CPP
