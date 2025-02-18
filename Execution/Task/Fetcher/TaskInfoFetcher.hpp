//
// Created by zxk on 6/7/23.
//

#ifndef OLVP_TASKINFOFETCHER_HPP
#define OLVP_TASKINFOFETCHER_HPP


#include "../../../Web/Restful/Client.hpp"
#include "../Id/TaskId.hpp"
#include "../TaskInfo.hpp"
#include <shared_mutex>
#include <thread>
#include "../../../Execution/Event/Event.h"



using std::this_thread::sleep_for;


class CpuUsageWindow {
    int windowLength = 0;

    vector<TaskCpuUsageDescriptor> window;

    int index = 0;
    double avgShuffleUsage = 0;
    double avgDriverUsage = 0;
    double avgAll = 0;

public:
    CpuUsageWindow(int windowLength)
    {
        this->windowLength = windowLength;
    }

    void put(TaskCpuUsageDescriptor desc)
    {
        if(index%windowLength >= window.size())
        {
            window.push_back(desc);
            index++;
            index = index%windowLength;
        }
        else
        {
            window[index] = desc;
            index++;
            index = index%windowLength;
        }
        avgShuffleUsage = getAvgShuffleUsage();
        avgDriverUsage = getAvgDriverUsage();
        avgAll = getAllUsage();

    }
    double getAllUsage()
    {
        double all = 0;
        for(auto n : this->window)
        {
            all +=n.getShuffleCpuUsage();
            all += n.getDriverCpuUsage();
        }
        return all / this->window.size();
    }
    double getAvgShuffleUsage()
    {
        double all = 0;
        for(auto n : this->window)
        {
            all +=n.getShuffleCpuUsage();
        }
        return all / this->window.size();

    }
    double getAvgDriverUsage()
    {
        double all = 0;
        for(auto n : this->window)
        {
            all +=n.getDriverCpuUsage();
        }
        return all / this->window.size();

    }
};

class throughputWindow
{

    int windowLength = 0;
    int shortBytesWindowlength = 3;
    int longBytesWindowlength = 40;

    vector<double> window;

    vector<double> bytesWindows;
    vector<double> longBytesWindows;
    int index = 0;
    int btyeindex = 0;
    int longByteindex = 0;
    double avg = 0;

public:
    throughputWindow(int windowLength)
    {
        this->windowLength = windowLength;
    }

    void put(double tp)
    {
        if(index%windowLength >= window.size())
        {
            window.push_back(tp);
            index++;
            index = index%windowLength;
        }
        else
        {
            window[index] = tp;
            index++;
            index = index%windowLength;
        }
        avg = getAvgTP();
    }

    void putThroughBytes(double tpBytes)
    {
        if(btyeindex%shortBytesWindowlength >= bytesWindows.size())
        {
            bytesWindows.push_back(tpBytes);
            btyeindex++;
            btyeindex = btyeindex%(shortBytesWindowlength);
        }
        else
        {
            bytesWindows[btyeindex] = tpBytes;
            btyeindex++;
            btyeindex = btyeindex%(shortBytesWindowlength);
        }

        if(longByteindex%longBytesWindowlength >= longBytesWindows.size())
        {
            longBytesWindows.push_back(tpBytes);
            longByteindex++;
            longByteindex = longByteindex%(longBytesWindowlength);
        }
        else
        {
            longBytesWindows[longByteindex] = tpBytes;
            longByteindex++;
            longByteindex = longByteindex%(longBytesWindowlength);
        }
    }
    double getAvgTP()
    {
        double all = 0;
        for(auto n : this->window)
        {
            all +=n;
        }
        return all / this->window.size();

    }
    double getAvgTPBytes()
    {
        double all = 0;
        for(auto n : this->bytesWindows)
        {
            all +=n;
        }
        return all / this->bytesWindows.size();

    }
    double getAvgLongTPBytes()
    {
        double all = 0;
        for(auto n : this->longBytesWindows)
        {
            all +=n;
        }
        return all / this->longBytesWindows.size();

    }

};



class BufferInfoWindow
{
    list<BufferInfoDescriptor> window;
    int windowLength = 20;
    int index = 0;

    enum trend{Expand,Shrink,Rest};

    trend taskBufferTrend = Rest;
    trend exchangBufferTrend = Rest;
    mutex lock;

public:
    BufferInfoWindow(int windowLength)
    {
        this->windowLength = windowLength;
    }
    void put(BufferInfoDescriptor des)
    {
        lock.lock();
        window.push_back(des);
        if(window.size() > windowLength)
            window.pop_front();
        lock.unlock();
    }

    void analyzeTrend()
    {

        lock.lock();
        int tBufferExpandCount = 0;
        int tBufferShrinkCount = 0;
        int eBufferExpandCount = 0;
        int eBufferShrinkCount = 0;



        if(this->window.size() == 1)
        {
            taskBufferTrend = Rest;
            exchangBufferTrend = Rest;
        }
        else {

            auto p= window.begin();
            auto pre = window.begin();
            for(;p!=window.end();p++){
                if(p == window.begin())
                    continue;
                if(p->getTb_TUp() != pre->getTb_TUp())
                    tBufferExpandCount++;
                if(p->getTb_TDown() != pre->getTb_TDown())
                    tBufferShrinkCount++;
                if(p->getEb_TUp() != pre->getEb_TUp())
                    eBufferExpandCount++;
                if(p->getEb_TDown() != pre->getEb_TDown())
                eBufferShrinkCount++;
                pre++;
            }

        }
        if(tBufferExpandCount == 0 && tBufferShrinkCount == 0)
            this->taskBufferTrend = Rest;
        else if(tBufferExpandCount > tBufferShrinkCount)
            this->taskBufferTrend = Expand;
        else
            this->taskBufferTrend = Shrink;

        if(eBufferExpandCount == 0 && eBufferShrinkCount == 0)
            this->exchangBufferTrend = Rest;
        else if(eBufferExpandCount > eBufferShrinkCount)
            this->exchangBufferTrend = Expand;
        else
            this->exchangBufferTrend = Shrink;

        lock.unlock();
    }

    bool isTaskBufferExpandTrend()
    {
        return this->taskBufferTrend == Expand;
    }
    bool isTaskBufferRestTrend()
    {
        return this->taskBufferTrend == Rest;
    }
    bool isTaskBufferShrinkTrend()
    {
        return this->taskBufferTrend == Shrink;
    }
    bool isExchangeBufferExpandTrend()
    {
        return this->exchangBufferTrend == Expand;
    }
    bool isExchangeBufferRestTrend()
    {
        return this->exchangBufferTrend == Rest;
    }
    bool isExchangeBufferShrinkTrend()
    {
        return this->exchangBufferTrend == Shrink;
    }


};


class RemainingTuplesWindowStruct
{
public:
    long tuples;
    long long timepoint;
    RemainingTuplesWindowStruct(long tuples,long long timepoint)
    {
        this->tuples = tuples;
        this->timepoint = timepoint;
    }

};

class RemainingTuplesWindow
{
    list<RemainingTuplesWindowStruct> window;

    int windowLength = 20;
    int index = 0;



    mutex lock;

public:
    RemainingTuplesWindow(int windowLength)
    {
        this->windowLength = windowLength;
    }
    void put(long remainingTuples,long long timeStamp)
    {
        lock.lock();

        RemainingTuplesWindowStruct rtws(remainingTuples,timeStamp);

        window.push_back(rtws);


        if(window.size() > windowLength){
            window.pop_front();
        }
        lock.unlock();
    }

    long getTimeGap()
    {
        long long maxTs = 0;
        long long minTs = 9999999999999999;
        for(auto item : this->window)
        {
            if(item.timepoint > maxTs)
                maxTs = item.timepoint;
            if(item.timepoint < minTs)
                minTs = item.timepoint;
        }
        return maxTs-minTs;
    }

    double getTupleConsumingRate()
    {
        if(this->window.size() == 0)
            return 0;

        return (getMaxRemainingTuples() - getMinRemainingTuples())/(this->window.size()*100);
    }

    long getMaxRemainingTuples()
    {

        lock.lock();
        long max = 0;
        for(auto tupleCount : this->window)
        {
            if(tupleCount.tuples > max)
                max = tupleCount.tuples;
        }
        lock.unlock();
        return max;
    }

    long getMinRemainingTuples()
    {

        lock.lock();
        long min = 999999999999999999;
        for(auto tupleCount : this->window)
        {
            if(tupleCount.tuples < min)
                min = tupleCount.tuples;
        }
        lock.unlock();
        return min;
    }


    int getRequestCount()
    {
        return this->window.size();
    }


};


class TaskInfoFetcher : public enable_shared_from_this<TaskInfoFetcher>
{
    shared_ptr<TaskId> taskId;
    string remoteTaskLocation;
    int delay = 100;
    shared_mutex lock;
    shared_ptr<TaskInfo> taskInfo = NULL;

    atomic<bool> finished = false;

    atomic<bool> abortFetch = false;

    shared_ptr<TaskThroughputInfo> taskThroughputInfo = make_shared<TaskThroughputInfo>();

    shared_ptr<throughputWindow> tw;
    shared_ptr<CpuUsageWindow> cw;

    double throughput = 0;
    double throughputBytes = 0;

    double maxRemainingTuples = 0;
    double remainingTuples = 0;
    double remainingBufferTuples = 0;
    double lastEnqueuedTuples = 0;
    double tupleReducingRate = 0.0;

    string buildRecord = "-1";
    double buildTime = -1;
    double buildComputingTime = -1;

    int requestCounter = 0;

    int bufferInfoRequestCounter = 0;
    shared_ptr<Event> eventListener;

    BufferInfoDescriptor bufferInfoDescriptor;
    shared_ptr<BufferInfoWindow> bufferDescWindow;

    shared_ptr<RemainingTuplesWindow> remainingTuplesWindow;


    int allThreadsNums = 0;

    bool hasThroughput = false;

    shared_ptr<RestfulClient> restfulClient = make_shared<RestfulClient>();

    map<string,double> joinIdToBuildTime;

    bool dependenciesSatisfied = false;

    double buildProgress = 0.0;

    long long stateMigratingFinishTimeStamp = -1;


public:
    TaskInfoFetcher(shared_ptr<TaskId> taskId,string remoteTaskLocation,shared_ptr<Event> eventListener)
    {
        this->taskId = taskId;
        this->remoteTaskLocation = remoteTaskLocation;

        tw = make_shared<throughputWindow>(20);
        cw = make_shared<CpuUsageWindow>(3);
        bufferDescWindow = make_shared<BufferInfoWindow>(50);

        ExecutionConfig config;
        int configSize = 50;
        if(config.getRemainingTupleWindowSize() != "NULL")
            configSize = atoi(config.getRemainingTupleWindowSize().c_str());

        remainingTuplesWindow = make_shared<RemainingTuplesWindow>(configSize);


        this->eventListener = eventListener;
    }
    shared_ptr<TaskInfo> getTaskInfo()
    {
        lock.lock_shared();
        shared_ptr<TaskInfo> result = this->taskInfo;
        lock.unlock_shared();

        return result;
    }

    bool sendRequest(string location,string path,shared_ptr<RestfulClient> client)
    {
        bool success = true;
        string linkString = location+path;
        string result = client->POST_GetResult(location,linkString,{this->taskId->ToString()});
        if(result == "NULL")
            return false;
        requestCounter++;
        bufferInfoRequestCounter++;
        lock.lock();
        this->taskInfo = TaskInfo::Deserialize(result);
        TaskThroughputInfo taskThroughputInfo1 = this->taskInfo->getTaskInfoDescriptor()->getTaskThroughputInfo();

        long a = (taskThroughputInfo1.getCurrentTupleCount() - this->taskThroughputInfo->getCurrentTupleCount());
        double b = (taskThroughputInfo1.getTimeStamp()-this->taskThroughputInfo->getTimeStamp());
        double c = (taskThroughputInfo1.getThroughputBytes() - this->taskThroughputInfo->getThroughputBytes());

        if(a > 0)
            this->hasThroughput = true;
        else
            this->hasThroughput = false;

        this->throughput = ((double)a)/b;
        this->throughputBytes = c/b;
        this->bufferInfoDescriptor = this->taskInfo->getTaskInfoDescriptor()->getBufferInfoDescriptor();

        tw->put(this->throughput);
        tw->putThroughBytes(this->throughputBytes);
        cw->put(this->taskInfo->getTaskInfoDescriptor()->getTaskCpuUsageDescriptor());
        this->allThreadsNums = this->taskInfo->getTaskInfoDescriptor()->getTaskCpuUsageDescriptor().getDriverNum() + this->taskInfo->getTaskInfoDescriptor()->getTaskCpuUsageDescriptor().getShufflerNum();
        this->bufferDescWindow->put(bufferInfoDescriptor);

        this->remainingTuples =  taskThroughputInfo1.getRemainingTuples();
        this->lastEnqueuedTuples = taskThroughputInfo1.getLastEnqueuedTuples();
        this->remainingTuplesWindow->put(this->remainingTuples,TimeCommon::getCurrentTimeStamp());


        if(taskThroughputInfo1.getRemainingBufferTuples() != this->remainingBufferTuples)
            this->bufferInfoRequestCounter = 1;

        this->remainingBufferTuples = taskThroughputInfo1.getRemainingBufferTuples();
        if(this->remainingTuples > this->maxRemainingTuples)
            this->maxRemainingTuples = this->remainingTuples;

        this->tupleReducingRate = (this->maxRemainingTuples-this->remainingTuples)/(delay * requestCounter);


   //     if(this->throughput > 0)
   //     {
   //         spdlog::debug("Through_Task_"+to_string(this->taskId->getStageId().getId())+"_"+ to_string(this->taskId->getId())+"===>"+ to_string(this->throughput));
   //     }
        this->taskThroughputInfo = make_shared<TaskThroughputInfo>(taskThroughputInfo1);



        if(this->buildRecord == "-1") {
            int joinNum = this->taskInfo->getTaskInfoDescriptor()->getJoinInfoDescriptor().getJoinNums();
            if (joinNum > 0) {
                int buildNum = this->taskInfo->getTaskInfoDescriptor()->getJoinInfoDescriptor().getBuildNums();

                long buildProgressCount = this->taskInfo->getTaskInfoDescriptor()->getJoinInfoDescriptor().getAllBuildProgress();
                long buildAllCount = this->taskInfo->getTaskInfoDescriptor()->getJoinInfoDescriptor().getAllBuildCount();
                if(buildAllCount == 0)
                    buildProgress = 100.0;
                else
                    buildProgress = ((double)buildProgressCount/(double)buildAllCount) *100.0;
                if (joinNum == buildNum) {
                    this->dependenciesSatisfied = true;
                    this->stateMigratingFinishTimeStamp = TimeCommon::getCurrentTimeStamp();
                    if (this->buildRecord == "-1") {
                        this->buildRecord = to_string(TimeCommon::getCurrentTimeStamp());
                        this->buildTime = this->taskInfo->getTaskInfoDescriptor()->getJoinInfoDescriptor().getBuildTime();
                        this->buildComputingTime = this->taskInfo->getTaskInfoDescriptor()->getJoinInfoDescriptor().getBuildComputingTime();
                        this->joinIdToBuildTime = this->taskInfo->getTaskInfoDescriptor()->getJoinInfoDescriptor().getJoinIdToBuildTime();
                    }
                }
            }
            else
                this->dependenciesSatisfied = true;
        }
        lock.unlock();

        return success;
    }
    string getBuildRecord()
    {
        return this->buildRecord;
    }



    double getBuildProgress()
    {
        return this->buildProgress;
    }

    long long getStateMigratingFinishTimeStamp(){
        return this->stateMigratingFinishTimeStamp;
    }

    double getBuildTime()
    {
        return this->buildTime;
    }
    double getBuildComputingTime()
    {
        return this->buildComputingTime;
    }

    bool isDependenciesSatisfied()
    {
        return this->dependenciesSatisfied;
    }

    bool taskHasThroughput()
    {
        return this->hasThroughput;
    }

    map<string,double> getJoinIdToBuildTime(){
        return this->joinIdToBuildTime;
    }

    double getThroughput()
    {
        return this->throughput;
    }
    double getAvgThroughput()
    {
        return tw->getAvgTP();
    }
    double getAvgThroughputBytes()
    {
        return tw->getAvgTPBytes()*1000.0;
    }
    double getLongAvgThroughputBytes()
    {
        return tw->getAvgLongTPBytes()*1000.0;
    }
    BufferInfoDescriptor getBufferInfoDesc()
    {
        return this->bufferInfoDescriptor;
    }

    double getAvgCpuUsage()
    {
        return cw->getAllUsage();
    }
    double getAvgShuffleCpuUsage()
    {
        return cw->getAvgShuffleUsage();
    }
    double getAvgDriverCpuUsage()
    {
        return cw->getAvgDriverUsage();
    }

    double getMaxRemainingTuple()
    {
        return this->remainingTuplesWindow->getMaxRemainingTuples();
    }


    double getRemainingTupleRequestTimeGap()
    {
        return this->remainingTuplesWindow->getTimeGap();
    }




    double getMinRemainingTuple()
    {
        return this->remainingTuplesWindow->getMinRemainingTuples();
    }
    double getLastEnqueuedTuples()
    {
        return this->lastEnqueuedTuples;
    }

    double getDelay()
    {
        return this->delay;
    }

    double getRemainingTuples()
    {
        return this->remainingTuples;
    }

    double getRemainingBufferTuples()
    {
        return this->remainingBufferTuples;
    }

    double getRequestCounter()
    {
        return this->remainingTuplesWindow->getRequestCount();
    }


    double getConsumingRate()
    {
        return this->remainingTuplesWindow->getTupleConsumingRate();
    }

    double getBufferRequestCounter()
    {
        return this->bufferInfoRequestCounter;
    }

    int getAllThreadsNums()
    {
        return this->allThreadsNums;
    }

    bool isTaskOutputBufferExpandTrend()
    {
        this->bufferDescWindow->analyzeTrend();
        return this->bufferDescWindow->isTaskBufferExpandTrend();
    }
    bool isTaskOutputBufferRestTrend()
    {
        this->bufferDescWindow->analyzeTrend();
        return this->bufferDescWindow->isTaskBufferRestTrend();
    }
    bool isTaskOutputBufferShrinkTrend()
    {
        this->bufferDescWindow->analyzeTrend();
        return this->bufferDescWindow->isTaskBufferShrinkTrend();
    }
    bool isTaskExchangeBufferExpandTrend()
    {
        this->bufferDescWindow->analyzeTrend();
        return this->bufferDescWindow->isExchangeBufferExpandTrend();
    }
    bool isTaskExchangeBufferRestTrend()
    {
        this->bufferDescWindow->analyzeTrend();
        return this->bufferDescWindow->isExchangeBufferRestTrend();
    }
    bool isTaskExchangeBufferShrinkTrend()
    {
        this->bufferDescWindow->analyzeTrend();
        return this->bufferDescWindow->isExchangeBufferShrinkTrend();
    }



    void start()
    {
        thread process([](shared_ptr<TaskInfoFetcher> fetcher) {
            while (true) {

                bool success = true;

                success = fetcher->sendRequest(fetcher->remoteTaskLocation, "/v1/task/getTaskInfo",
                                               fetcher->restfulClient);


                if (fetcher->taskInfo != NULL && fetcher->taskInfo->getStatus().compare("FINISHED") == 0) {
                    break;
                }
                sleep_for(std::chrono::milliseconds(fetcher->delay));

                if(!success) {
                    string error = "TaskInfoFetcher Error! ";
                    spdlog::critical(error);
                    //break;
                }

            }
            fetcher->restfulClient = NULL;
            fetcher->finished = true;
            fetcher->eventListener->notify();
            fetcher->hasThroughput = false;
        },shared_from_this());
        process.detach();
    }

    void abort()
    {

    }

    bool isDone()
    {
        return this->finished;
    }




};


#endif //OLVP_TASKINFOFETCHER_HPP
