//
// Created by zxk on 11/14/23.
//

#ifndef OLVP_TASKTHROUGHPUTINFO_H
#define OLVP_TASKTHROUGHPUTINFO_H

#include "../../../Utils/TimeCommon.hpp"
class TaskThroughputInfo
{
    long currentTupleCount;
    long long timeStamp;
    long remainingTuples;
    long remainingBufferTuples;
    long lastEnqueuedTuples;
    long throughputBytes;
    long totalTuplesBytes;
public:
    TaskThroughputInfo()
    {
        this->currentTupleCount = 0;
        this->throughputBytes = 0;
        this->timeStamp = 0;
        this->remainingBufferTuples = 0;
        this->remainingTuples = -1;
        this->lastEnqueuedTuples = -1;
        this->totalTuplesBytes = 0;
    }
    /*
    TaskThroughputInfo(long count,long bytes)
    {
        this->currentTupleCount = count;
        this->throughputBytes = bytes;
        this->timeStamp = TimeCommon::getCurrentTimeStamp();

        this->remainingTuples = -1;
        this->lastEnqueuedTuples = -1;
        this->remainingBufferTuples = 0;
    }
    TaskThroughputInfo(long count,long bytes,long remainingBufferTuples,long remainingTuples)
    {
        this->currentTupleCount = count;
        this->throughputBytes = bytes;
        this->timeStamp = TimeCommon::getCurrentTimeStamp();
        this->throughputBytes = 0;
        this->remainingTuples = remainingTuples;
        this->remainingBufferTuples = remainingBufferTuples;
        this->lastEnqueuedTuples = -1;
    }
     */
    TaskThroughputInfo(long count,long bytes,long remainingBufferTuples,long remainingTuples, long lastEnqueuedTuples,long totalTupleBytes)
    {
        this->currentTupleCount = count;
        this->throughputBytes = bytes;
        this->timeStamp = TimeCommon::getCurrentTimeStamp();
        this->remainingTuples = remainingTuples;
        this->remainingBufferTuples = remainingBufferTuples;
        this->lastEnqueuedTuples = lastEnqueuedTuples;
        this->totalTuplesBytes = totalTupleBytes;
    }


    TaskThroughputInfo(long count,long bytes,long long timeStamp, long remainingBufferTuples,long remainingTuples, long lastEnqueuedTuples,long totalTupleBytes)
    {
        this->currentTupleCount = count;
        this->throughputBytes = bytes;
        this->timeStamp = timeStamp;
        this->remainingTuples = remainingTuples;
        this->remainingBufferTuples = remainingBufferTuples;
        this->lastEnqueuedTuples = lastEnqueuedTuples;
        this->totalTuplesBytes = totalTupleBytes;
    }
    /*
      TaskThroughputInfo(long count,long bytes,long long timeStamp)
      {
          this->currentTupleCount = count;
          this->throughputBytes = bytes;
          this->timeStamp = timeStamp;
          this->remainingTuples = -1;
          this->lastEnqueuedTuples = -1;
      }
  */

    long getCurrentTupleCount()
    {
        return this->currentTupleCount;
    }
    long getThroughputBytes()
    {
        return this->throughputBytes;
    }
    long long getTimeStamp()
    {
        return this->timeStamp;
    }

    long getRemainingTuples()
    {
        return this->remainingTuples;
    }

    long getLastEnqueuedTuples()
    {
        return this->lastEnqueuedTuples;
    }

    long getRemainingBufferTuples()
    {
        return this->remainingBufferTuples;
    }

    long getTotalTuplesBytes()
    {
        return this->totalTuplesBytes;
    }

    static string Serialize(TaskThroughputInfo desc)
    {
        nlohmann::json json;

        json["currentTupleCount"] = desc.currentTupleCount;
        json["timeStamp"] = desc.timeStamp;
        json["remainingTuples"] = desc.remainingTuples;
        json["remainingBufferTuples"] = desc.remainingBufferTuples;
        json["lastEnqueuedTuples"] = desc.lastEnqueuedTuples;
        json["throughputBytes"] = desc.throughputBytes;
        json["throughputBytes"] = desc.totalTuplesBytes;

        string result = json.dump();
        return result;
    }

    static shared_ptr<TaskThroughputInfo> Deserialize(string desc)
    {
        nlohmann::json json = nlohmann::json::parse(desc);
        vector<shared_ptr<TaskThroughputInfo>> pipelineDescriptors;


        return make_shared<TaskThroughputInfo>(json["currentTupleCount"],json["throughputBytes"],json["timeStamp"],json["remainingBufferTuples"],json["remainingTuples"],json["lastEnqueuedTuples"],json["throughputBytes"]);
    }




};

#endif //OLVP_TASKTHROUGHPUTINFO_H
