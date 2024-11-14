//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_TASKCONTEXT_H
#define OLVP_TASKCONTEXT_H

#include <memory>
#include <list>
#include <map>

using namespace std;

#include "../../Buffer/OutputBuffer.hpp"
#include "../TaskStateMachine.hpp"
#include "../../../Planner/LocalPlanner/PipelineId.hpp"
#include "../Id/TaskId.hpp"
#include "TaskThroughputInfo.h"
#include "../../../Session/RuntimeConfigParser.hpp"
#include "../TasksRuntimeStats.hpp"
#include "../../../Execution/Buffer/BufferInfoDescriptor.hpp"
#include "../../../Execution/Task/TaskInfos/JoinInfoDescriptor.hpp"
class PipelineContext;
class QueryContext;
class OutputBuffer;
class TaskContext:public std::enable_shared_from_this<TaskContext>
{

    mutex lock;

    map<PipelineId,shared_ptr<PipelineContext>> pipelineContexts;
    set<shared_ptr<PipelineContext>> activePipelineContexts;

    shared_ptr<TaskStateMachine> stateMachine;
    weak_ptr<QueryContext> queryContext;

    shared_ptr<OutputBuffer> outputBuffer = NULL;

    shared_ptr<TaskId> taskId;
    atomic<long> currentTotalTupleCount = 0;
    atomic<long> remainingTableTupleCount = 0;
    atomic<long> bufferRemainingTupleCount = 0;
    atomic<bool> remainingTupleReport = true;

    shared_ptr<TasksRuntimeStats> tasksRuntimeStats;
    atomic<long> lastEnqueuedTupleCount = 0;


    int buildNums = 0;

    shared_ptr<std::chrono::system_clock::time_point> firstStartBuildTime = NULL;
    shared_ptr<std::chrono::system_clock::time_point> lastBuildFinishedTime = NULL;

    atomic<double> maxBuildTime = -1;
    atomic<double> maxBuildComputingTime =-1;

    mutex tidLock;
    set<__pid_t> shuffleExecutorTids;

    set<__pid_t> allTids;

    int outputTupleByteWidth = -1;

    int joinNum = 0;

    int totalInputBytes = 0;


    atomic<int> bufferSizeTurnDownCounter = 0;
    atomic<int> bufferSizeTurnUpCounter = 0;
    atomic<int> exchangeBufferSizeTurnDownCounter = 0;
    atomic<int> exchangeBufferSizeTurnUpCounter = 0;

public:
    TaskContext(shared_ptr<TasksRuntimeStats> tasksRuntimeStats,weak_ptr<QueryContext> queryContext,shared_ptr<TaskId> taskId,shared_ptr<TaskStateMachine> stateMachine,shared_ptr<OutputBuffer> outputBuffer);
    shared_ptr<PipelineContext> addPipelineContext(PipelineId pipelineId);
    void finishPipelineContext(PipelineId pipelineId);
    map<PipelineId,shared_ptr<PipelineContext>> getPipelineContexts();
    shared_ptr<PipelineContext> getPipelineContext(PipelineId pipelineId);
    shared_ptr<TaskStateMachine> getState();
    shared_ptr<OutputBuffer> getOutputBuffer();
    double getRunningTime();
    TaskThroughputInfo getTaskThroughputInfo();
    void addTotalTupleCount(long increment);

    void addTotalInputTuples(int increment)
    {
        this->totalInputBytes += (increment*this->outputTupleByteWidth);
    }
    long getTotalInputTuples()
    {
        return this->totalInputBytes;
    }

    void addBufferSizeTurnUpCounter()
    {
        this->bufferSizeTurnUpCounter++;
        if(this->bufferSizeTurnUpCounter >= INT32_MAX)
            this->bufferSizeTurnUpCounter = 0;
    }
    void addBufferSizeTurnDownCounter()
    {
        this->bufferSizeTurnDownCounter++;
        if(this->bufferSizeTurnDownCounter >=  INT32_MAX)
            this->bufferSizeTurnDownCounter = 0;
    }

    void addExchangeBufferSizeTurnUpCounter()
    {
        this->exchangeBufferSizeTurnUpCounter++;
        if(this->exchangeBufferSizeTurnUpCounter >=  INT32_MAX)
            this->exchangeBufferSizeTurnUpCounter = 0;
    }
    void addExchangeBufferSizeTurnDownCounter()
    {
        this->exchangeBufferSizeTurnDownCounter++;
        if(this->exchangeBufferSizeTurnDownCounter >= INT32_MAX)
            this->exchangeBufferSizeTurnDownCounter = 0;
    }

    BufferInfoDescriptor getBufferInfoDescriptor()
    {
        return BufferInfoDescriptor(this->bufferSizeTurnDownCounter,this->bufferSizeTurnUpCounter,
                                    this->exchangeBufferSizeTurnUpCounter,this->exchangeBufferSizeTurnDownCounter);
    }

    JoinInfoDescriptor getJoinInfoDescriptor();

    int getOutputTupleWidth()
    {
        return this->outputTupleByteWidth;
    }

    void setOutputTupleWidth(int width)
    {
        this->outputTupleByteWidth = width;
    }

    void updateRemainingBufferTupleCount(long count)
    {
        this->bufferRemainingTupleCount += count;
    }

    void setLastEnqueuedTupleCount(long count){
        this->lastEnqueuedTupleCount = count;
    }

    long getRemainingTableTupleCount();
    long getRemainingBufferTupleCount();
    void setRemainingTableTupleCount(long count);
    shared_ptr<RuntimeConfigParser> getRuntimeConfigs();

    void increaseThreadNums()
    {
        this->tasksRuntimeStats->increaseThreadNums();
    }
    void decreaseThreadNums()
    {
        this->tasksRuntimeStats->decreaseThreadNums();
    }
    void setJoinNum(int join)
    {
        this->joinNum = join;
    }

    int getJoinNums()
    {
        return this->joinNum;
    }
    void reportBuildComplete()
    {
        this->buildNums++;
        if(this->buildNums == this->joinNum)
        {
            if(this->lastBuildFinishedTime != NULL && this->firstStartBuildTime != NULL) {
                double duration_millsecond = std::chrono::duration<double, std::milli>(
                        *this->lastBuildFinishedTime - *this->firstStartBuildTime).count();
                spdlog::debug("Total build time interval in this task is "+to_string(duration_millsecond) +" !!");
            }
        }
    }

    shared_ptr<TaskId> getTaskId()
    {
        return this->taskId;
    }
    double getBuildTime()
    {
        if(this->buildNums == this->joinNum)
        {
            return this->maxBuildTime;
        }
        return -1;
    }

    double getBuildComputingTime()
    {
        if(this->buildNums == this->joinNum)
        {
            return this->maxBuildComputingTime;
        }
        return -1;
    }


    bool hasBuildTask()
    {
        return this->joinNum > 0? true:false;
    }
    bool isAllBuildCompeletedInTask()
    {

        return this->buildNums == this->joinNum? true:false;
    }

    int getBuildNums()
    {
        return this->buildNums;
    }

    void reportBuildStartTime(std::chrono::system_clock::time_point time);

    void reportBuildFinishedTime(std::chrono::system_clock::time_point time);


    void reportBuildTime(double time);
    void reportBuildComputingTime(double time);

    map<string,set<__pid_t>> getAllTaskTids();



    void addTids(set<__pid_t> ids);

    void removeTids(set<__pid_t> ids);

};
#endif //OLVP_TASKCONTEXT_H
