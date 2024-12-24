//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_DRIVERCONTEXT_CPP
#define OLVP_DRIVERCONTEXT_CPP



#include "PipelineContext.h"
#include "DriverContext.h"


#include "../../../Operators/RemoteSourceOperator.hpp"
DriverContext::DriverContext(weak_ptr<PipelineContext> pipelineContext){
    this->pipelineContext = pipelineContext;
}

weak_ptr<PipelineContext> DriverContext::getPipelineContext() {
    return this->pipelineContext;
}

bool DriverContext::hasDriver() {
    return this->driver == NULL;
}

void DriverContext::setDriver(shared_ptr<vector<shared_ptr<Operator>>> physicalPipeline) {
    this->driver = physicalPipeline;
}

void DriverContext::addRemoteSourceLocation(set<std::shared_ptr<Split>> scheduledSplits) {

    if(scheduledSplits.size() > 0)
    {
        for(int i = 0 ; i < (*this->driver).size() ; i++) {

            if ((*this->driver)[i]->getOperatorId() == "RemoteSourceOperator") {

                static_pointer_cast<RemoteSourceOperator>((*this->driver)[i])->addSources(scheduledSplits);
                break;
            }
        }
    }
}

void DriverContext::closeRemoteSourceDriver() {

    for (int i = 0; i < (*this->driver).size(); i++) {

        if ((*this->driver)[i]->getOperatorId() == "RemoteSourceOperator") {

            static_pointer_cast<RemoteSourceOperator>((*this->driver)[i])->abort();
            break;
        }
    }

}
void DriverContext::addRuntimeLocation(set<std::shared_ptr<Split>> scheduledSplits) {

    if(this->driver == NULL)
        this->cachedLocations.push_back(scheduledSplits);
    else
        this->addRemoteSourceLocation(scheduledSplits);

}

void DriverContext::releaseCachedLocations() {

    for(auto location : this->cachedLocations)
        this->addRemoteSourceLocation(location);

}

shared_ptr<TableScanRecord> DriverContext::getTableScanRecord() {
    return this->tableScanRecord;
}

void DriverContext::recordTableScanInfo(shared_ptr<TableScanRecord> tableScanRecord) {
    this->tableScanRecord = tableScanRecord;
}

shared_ptr<OutputBuffer> DriverContext::getOutputBuffer() {
    return this->pipelineContext.lock()->getOutputBuffer();
}

void DriverContext::addRemotePageCount()
{
    this->remotePagesReceived++;
}

void DriverContext::setRemainingTableTupleCount(long count)
{
    this->pipelineContext.lock()->setRemainingTableTupleCount(count);
}

shared_ptr<RuntimeConfigParser> DriverContext::getRuntimeConfigs()
{
    return this->pipelineContext.lock()->getRuntimeConfigs();
}


void DriverContext::reportBuildComplete(){
    this->pipelineContext.lock()->reportBuildComplete();
}

bool DriverContext::isAllBuildCompeletedInTask() {
    return this->pipelineContext.lock()->isAllBuildCompeletedInTask();
}
bool DriverContext::hasBuildTask() {
    return this->pipelineContext.lock()->hasBuildTask();
}

void DriverContext::regRemoteSplit(set<shared_ptr<Split>> splits)
{
    this->pipelineContext.lock()->regRemoteSplit(splits);
}

void DriverContext::reportBuildStartTime(std::chrono::system_clock::time_point time){
    this->pipelineContext.lock()->reportBuildStartTime(time);
}

void DriverContext::reportBuildFinishedTime(std::chrono::system_clock::time_point time){
    this->pipelineContext.lock()->reportBuildFinishedTime(time);
}
void DriverContext::reportBuildTime(double time){
    this->pipelineContext.lock()->reportBuildTime(time);
};

void DriverContext::reportBuildComputingTime(string joinId,double time){
    this->pipelineContext.lock()->reportBuildComputingTime(joinId,time);
};


void DriverContext::addTids(set<__pid_t> ids)
{
    this->pipelineContext.lock()->addDriverTids(ids);
}
void DriverContext::removeTids(set<__pid_t> ids)
{
    this->pipelineContext.lock()->removeDriverTids(ids);
}

void DriverContext::addExchangeBufferTurnUpCounter(){
    this->pipelineContext.lock()->addExchangeBufferTurnUpCounter();
}
void DriverContext::addExchangeBufferTurnDownCounter(){
    this->pipelineContext.lock()->addExchangeBufferTurnDownCounter();
}


atomic<long> &DriverContext::getBuildAllCount()
{
return this->pipelineContext.lock()->getAllBuildCount();
}
atomic<long> &DriverContext::getBuildProgress()
{
return this->pipelineContext.lock()->getAllBuildProgress();
}

#endif //OLVP_DRIVERCONTEXT_CPP
