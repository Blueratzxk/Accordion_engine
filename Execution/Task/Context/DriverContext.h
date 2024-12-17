//
// Created by zxk on 6/6/23.
//

#ifndef OLVP_DRIVERCONTEXT_H
#define OLVP_DRIVERCONTEXT_H


#include <memory>
#include <list>
#include "../../../Operators/Operator.hpp"
#include "../../../Split/Split.hpp"

#include "TableScanRecord.hpp"
#include "../../Buffer/OutputBuffer.hpp"
#include "../Session/RuntimeConfigParser.hpp"
using namespace std;

class PipelineContext;

class DriverContext : public enable_shared_from_this<DriverContext>
{

    weak_ptr<PipelineContext> pipelineContext;
    shared_ptr<vector<shared_ptr<Operator>>> driver = NULL;

    vector<set<std::shared_ptr<Split>>> cachedLocations;

    shared_ptr<TableScanRecord> tableScanRecord = NULL;

    int remotePagesReceived = 0;

    string downStreamHaveJoin = "NONE";

    long buildAllCount = -1;
    long buildProgress = -1;

public:
    DriverContext(weak_ptr<PipelineContext> pipelineContext);

    weak_ptr<PipelineContext> getPipelineContext();

    void setDownStreamHaveJoin(string val)
    {
        this->downStreamHaveJoin = val;
    }
    string getDownStreamHaveJoin(){
        return this->downStreamHaveJoin;
    }

    atomic<long> &getBuildAllCount();

    atomic<long> &getBuildProgress();



    void setDriver(shared_ptr<vector<shared_ptr<Operator>>> physicalPipeline);

    void addRemoteSourceLocation(set<std::shared_ptr<Split>> scheduledSplits);

    void closeRemoteSourceDriver();

    void addRuntimeLocation(set<std::shared_ptr<Split>> scheduledSplits);

    bool hasDriver();

    void recordTableScanInfo(shared_ptr<TableScanRecord> tableScanRecord);

    void releaseCachedLocations();

    shared_ptr<TableScanRecord> getTableScanRecord();

    shared_ptr<OutputBuffer> getOutputBuffer();

    void addRemotePageCount();

    void setRemainingTableTupleCount(long count);

    shared_ptr<RuntimeConfigParser> getRuntimeConfigs();


    void reportBuildComplete();

    bool isAllBuildCompeletedInTask();

    bool hasBuildTask();

    void regRemoteSplit(set<shared_ptr<Split>>);

    void reportBuildStartTime(std::chrono::system_clock::time_point time);

    void reportBuildFinishedTime(std::chrono::system_clock::time_point time);

    void reportBuildTime(string joinId,double time);

    void reportBuildComputingTime(double time);

    void addTids(set<__pid_t> ids);
    void removeTids(set<__pid_t> ids);

    void addExchangeBufferTurnUpCounter();
    void addExchangeBufferTurnDownCounter();
};

#endif //OLVP_DRIVERCONTEXT_H
