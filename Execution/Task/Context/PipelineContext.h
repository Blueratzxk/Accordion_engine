//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_PIPELINECONTEXT_H
#define OLVP_PIPELINECONTEXT_H

#include <memory>
#include <list>
#include <mutex>
#include <atomic>
#include <set>
#include "../../../Planner/LocalPlanner/PipelineId.hpp"
#include "../../Buffer/OutputBuffer.hpp"
#include "../../../Session/RuntimeConfigParser.hpp"
#include "../../../Split/Split.hpp"
#include "../../../Split/RemoteSplit.hpp"
using namespace std;
class TaskContext;
class DriverContext;
class PipelineContext:public std::enable_shared_from_this<PipelineContext>
{
    list<shared_ptr<DriverContext>> driverContexts;
    set<shared_ptr<DriverContext>> runningContexts;

    set<shared_ptr<Split>> AllRemoteSplits;
    mutex splitsLock;

    weak_ptr<TaskContext> taskContext;
    PipelineId pipelineId;

    atomic<int> threadNums = 0;

    atomic<bool> pipelineFinished = false;
    mutex lock;

    mutex tidLock;
    set<__pid_t> tids;

public:
    PipelineContext(weak_ptr<TaskContext> taskContext,PipelineId pipelineId);
    shared_ptr<DriverContext> addDriverContext();
    void finishDriverContext(shared_ptr<DriverContext>);
    list<shared_ptr<DriverContext>> getDriverContexts();
    weak_ptr<TaskContext> getTaskContext();
    PipelineId getPipelineId();
    shared_ptr<OutputBuffer> getOutputBuffer();

    void setRemainingTableTupleCount(long count);
    shared_ptr<RuntimeConfigParser> getRuntimeConfigs();
    void increaseThreadNums();
    void decreaseThreadNums();

    void reportBuildComplete();

    void regRemoteSplit(set<shared_ptr<Split>>);
    int getAllRegRemoteSplitCount();

    set<shared_ptr<Split>> getAllRegRemoteSplit();

    bool isAllBuildCompeletedInTask();

    bool hasBuildTask();

    int getRunningDriverCount();

    void addExchangeBufferTurnUpCounter();
    void addExchangeBufferTurnDownCounter();

    void reportBuildStartTime(std::chrono::system_clock::time_point time);

    void reportBuildFinishedTime(std::chrono::system_clock::time_point time);

    void reportBuildTime(string joinId,double time);
    void reportBuildComputingTime(double time);


    void addDriverTids(set<__pid_t> ids)
    {
        this->tidLock.lock();
        for(auto id : ids)
            this->tids.insert(id);
        this->tidLock.unlock();
    }
    set<__pid_t> getPipelineTids()
    {
        this->tidLock.lock();
        auto ids = this->tids;
        this->tidLock.unlock();
        return ids;
    }
    void removeDriverTids(set<__pid_t> ids)
    {
        this->tidLock.lock();
        for(auto id : ids)
            this->tids.erase(id);
        this->tidLock.unlock();
    }

};

#endif //OLVP_PIPELINECONTEXT_H
