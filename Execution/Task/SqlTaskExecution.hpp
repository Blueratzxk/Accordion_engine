//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_SQLTASKEXECUTION_HPP
#define OLVP_SQLTASKEXECUTION_HPP


//#include "../../common.h"
#include "TaskStateMachine.hpp"

#include "Context/TaskContext.h"
#include "../Buffer/OutputBuffer.hpp"
#include "../../Planner/LocalPlanner/LogicalPipelineFactory.hpp"
#include "../../Operators/Join/JoinBridge.hpp"
#include "../../Frontend/PlanNode/PlanNodeId.hpp"
#include "../../Operators/LocalExchange/LocalExchange.hpp"

#include "TaskHandle.hpp"

#include "TaskExecutor/TaskExecutor.hpp"
#include "LogicalPipelineRunner.hpp"
#include "TaskSource.hpp"
#include "../Task/TaskInfos/PipelineDescriptor.hpp"
#include "../../Operators/LogicalOperators/Logical_LocalExchangeSinkOperator.hpp"

#include <future>

using namespace std::chrono;

class SqlTaskExecution {

    shared_ptr<Session> session;
    std::shared_ptr<TaskId> taskId = NULL;
    std::shared_ptr<TaskStateMachine> taskStateMachine = NULL;
    std::shared_ptr<TaskContext> taskContext = NULL;
    std::shared_ptr<OutputBuffer> outputBuffer = NULL;
    std::shared_ptr<TaskExecutor> taskExecutor = NULL;
    std::shared_ptr<TaskHandle> taskHandle = NULL;



    map<PlanNodeId,PipelineId> sourcePlanNodeId_To_LPipeline;

    map<PipelineId, pair<std::shared_ptr<LogicalPipeline>,int>> sourceLogicalPipelineRegister;//记录所有source logicalpipeline
    map<PipelineId, pair<std::shared_ptr<LogicalPipeline>, int>> tableScanLogicalPipelineRegister;//记录所有tablescan logical pipeline
    map<PipelineId, pair<std::shared_ptr<LogicalPipeline>, int>> remoteSourceLogicalPipelineRegister;


    map<PipelineId,shared_ptr<LogicalPipelineRunnerFactory>> sourcePipelineFactory;
    map<PipelineId,shared_ptr<LogicalPipelineRunnerFactory>> tableScanPipelineFactory;
    map<PipelineId,shared_ptr<LogicalPipelineRunnerFactory>> remoteSourcePipelineFactory;



    map<string,shared_ptr<JoinBridge>> joinBridges;
    map<string, shared_ptr<LocalExchangeFactory>> localExchanges;//记录localExchange



    PipelineId outputPipelineName;
    high_resolution_clock::time_point  start;

    int initial_runtime_scalable_pipeline_concurrent = 3;


public:


    SqlTaskExecution(shared_ptr<Session> session,std::shared_ptr<TaskId> taskId,std::shared_ptr<TaskStateMachine> taskStateMachine,std::shared_ptr<TaskContext> taskContext,
    std::shared_ptr<OutputBuffer> outputBuffer, std::shared_ptr<TaskExecutor> taskExecutor)
    {
        this->session = session;
        this->taskId = taskId;
        this->outputBuffer = outputBuffer;
        this->taskStateMachine = taskStateMachine;
        this->taskExecutor = taskExecutor;
        this->taskContext = taskContext;
        ExecutionConfig config;
        int concur = atoi(config.getInitial_intra_task_concurrency().c_str());
        this->initial_runtime_scalable_pipeline_concurrent = concur;
    }
    //-------------------------------------------------------------------------------------------------------------//
    void setClock()
    {
        start = high_resolution_clock::now();
    }

    bool isRuntimeScalablePipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        /*
        auto operators = logicalPipeline->getLogicalPipelines();
        auto opTail = operators.back();
        auto opHead = operators.Frontend();

        if(opHead->getTypeId() == "Logical_LocalExchangeSourceOperator" && opTail->getTypeId() == "Logical_LocalExchangeSinkOperator")
            return true;
        return false;
        */
        auto operators = logicalPipeline->getLogicalPipelines();

        for(auto op : operators)
        {
            if(op->getTypeId() == "Logical_NestedLoopBuildOperator" ||op->getTypeId() == "Logical_HashBuilderOperator" ||op->getTypeId() == "Logical_TableScanOperator" ||op->getTypeId() == "Logical_FinalAggregationOperator" || op->getTypeId() == "Logical_TopKOperator" || op->getTypeId() == "Logical_SortOperator")
                return false;
        }
        return true;
    }


    bool isBuildPipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        auto operators = logicalPipeline->getLogicalPipelines();

        for(auto op : operators)
        {
            if(op->getTypeId() == "Logical_NestedLoopBuildOperator" ||op->getTypeId() == "Logical_HashBuilderOperator")
                return true;
        }
        return false;
    }

    bool isRemoteSinkPipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        auto operators = logicalPipeline->getLogicalPipelines();
        auto opTail = operators.back();
        auto opHead = operators.front();

        if(opHead->getTypeId() == "Logical_RemoteSourceOperator" && opTail->getTypeId() == "Logical_LocalExchangeSinkOperator")
            return true;
        return false;
    }
    bool isHashExchangePipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        auto operators = logicalPipeline->getLogicalPipelines();
        auto opTail = operators.back();

        shared_ptr<Logical_LocalExchangeSinkOperator> op = dynamic_pointer_cast<Logical_LocalExchangeSinkOperator>(opTail);

        return (op->getExchangeType() == "hash");

    }
    bool isRemoteOutputPipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        auto operators = logicalPipeline->getLogicalPipelines();
        auto opTail = operators.back();
        auto opHead = operators.front();

        if(opHead->getTypeId() == "Logical_RemoteSourceOperator" && opTail->getTypeId() == "Logical_TaskOutputOperator")
            return true;
        return false;
    }

    bool isRemoteSourcePipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        auto operators = logicalPipeline->getLogicalPipelines();

        auto opHead = operators.front();

        if(opHead->getTypeId() == "Logical_RemoteSourceOperator")
            return true;
        return false;
    }


    bool isSourceOutputPipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        auto operators = logicalPipeline->getLogicalPipelines();

        auto opTail = operators.back();
        auto opHead = operators.front();
        if(opHead->getTypeId() == "Logical_LocalExchangeSourceOperator" && opTail->getTypeId() == "Logical_TaskOutputOperator")
            return true;
        return false;

    }
    bool isSourcePipeline(std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        auto operators = logicalPipeline->getLogicalPipelines();

        auto opTail = operators.back();
        auto opHead = operators.front();
        if(opHead->getTypeId() == "Logical_LocalExchangeSourceOperator")
            return true;
        return false;

    }


    std::shared_ptr<TaskContext> getTaskContext()
    {
        return this->taskContext;
    }


    void createLogicalRunnerFactories()
    {

        for(auto source : this->sourceLogicalPipelineRegister)
        {
            this->sourcePipelineFactory[source.first] = std::make_shared<LogicalPipelineRunnerFactory>(this->session,this->taskContext->getPipelineContext(source.first),source.second.first);
        }
        for(auto remote : this->remoteSourceLogicalPipelineRegister)
        {
            this->remoteSourcePipelineFactory[remote.first] = std::make_shared<LogicalPipelineRunnerFactory>(this->session,this->taskContext->getPipelineContext(remote.first),remote.second.first);
        }
        for(auto tableScan : this->tableScanLogicalPipelineRegister)
        {
            this->tableScanPipelineFactory[tableScan.first] = std::make_shared<LogicalPipelineRunnerFactory>(this->session,this->taskContext->getPipelineContext(tableScan.first),tableScan.second.first);
        }


    }




    void RegATableScanLogicalPipeline(PipelineId pipeLineName, shared_ptr<LogicalPipeline> pipeline,vector<int> pipelineTypes,int initialConcurrent)//记录一个tablescanpipeline以及初始并发度
    {
        this->tableScanLogicalPipelineRegister[pipeLineName] = make_pair(pipeline ,initialConcurrent);
        this->taskContext->addPipelineContext(pipeLineName);
    }

    void RegOutputPipeline(PipelineId pipeLineName)
    {
        if(this->outputPipelineName.get() != "NULL")
            spdlog::critical("ERROR! A task only have one OutputPipeline!");

        this->outputPipelineName = pipeLineName;
    }


    void RegSourceLogicalPipeline(PipelineId pipeLineName, shared_ptr<LogicalPipeline> pipeline,vector<int> pipelineTypes,int initialConcurrent)//记录一个sourcepipeline以及初始并发度
    {
        this->sourceLogicalPipelineRegister[pipeLineName] = make_pair(pipeline, initialConcurrent);
        this->taskContext->addPipelineContext(pipeLineName);
    }

    void RegRemoteSourceLogicalPipeline(PipelineId pipeLineName,shared_ptr<LogicalPipeline> pipeline,vector<int> pipelineTypes,int initialConcurrent)//记录一个sourcepipeline以及初始并发度
    {
        this->remoteSourceLogicalPipelineRegister[pipeLineName] = make_pair(pipeline, initialConcurrent);
        this->taskContext->addPipelineContext(pipeLineName);
    }

    void RegCPUJoinBridge(string bridgeName, shared_ptr<JoinBridge> bridge)//记录一个CPU join桥
    {
        this->joinBridges[bridgeName] = bridge;
    }


    void RegLocalExchange(string sourcePipelineId,shared_ptr<LocalExchangeFactory> localExchangeFactory)
    {
        this->localExchanges[sourcePipelineId] = localExchangeFactory;
    }


    void Reg_SourcePlanNodeId_To_LPipeline(PlanNodeId planNodeId,string lPipelineName)
    {
        this->sourcePlanNodeId_To_LPipeline[planNodeId] = lPipelineName;
    }

    //-------------------------------------------------------------------------------------------------------------//
    bool isGPUPipeline(PipelineId pipelineName)
    {
        string::size_type idGPU,idCPU;
        idGPU = pipelineName.get().find("GPU");
        idCPU = pipelineName.get().find("CPU");

        if (idGPU != string::npos && idCPU == string::npos)
            return true;
        else if (idGPU == string::npos && idCPU != string::npos)
            return false;
        else
        {
            cout << "pipeline name define error,need identify GPU or CPU Pipeline!!!!" << endl;
            exit(0);
        }

    }
    bool isCPUPipeline(PipelineId pipelineName)
    {
        string::size_type idGPU, idCPU;
        idGPU = pipelineName.get().find("GPU");
        idCPU = pipelineName.get().find("CPU");

        if (idGPU == string::npos && idCPU != string::npos)
            return true;
        else if (idGPU != string::npos && idCPU == string::npos)
            return false;
        else
        {
            cout << "pipeline name define error,need identify GPU or CPU Pipeline!!!!" << endl;
            exit(0);
        }
    }

    bool isSourcePipeline(PipelineId pipelineName)
    {
        string::size_type idSource, idTableScan;
        idSource = pipelineName.get().find("Source");
        idTableScan = pipelineName.get().find("TableScan");

        if (idSource == string::npos && idTableScan != string::npos)
            return false;
        else if (idSource != string::npos && idTableScan == string::npos)
            return true;
        else
        {
            cout << "pipeline name define error,need identify source or tablescan Pipeline!!!!" << endl;
            exit(0);
        }
    }

    bool isTableScanPipeline(PipelineId pipelineName)
    {
        string::size_type idSource, idTableScan;
        idSource = pipelineName.get().find("Source");
        idTableScan = pipelineName.get().find("TableScan");

        if (idSource != string::npos && idTableScan == string::npos)
            return false;
        else if (idSource == string::npos && idTableScan != string::npos)
            return true;
        else
        {
            cout << "pipeline name define error,need identify source or tablescan Pipeline!!!!" << endl;
            exit(0);
        }
    }



    void startTask()
    {

        this->taskStateMachine->start();
        this->taskHandle = this->taskExecutor->addTask(this->taskId);

        vector<std::shared_ptr<SplitRunner>> runners;
        for(auto source : this->sourcePipelineFactory)
        {
            if(isSourceOutputPipeline(source.second->getLogicalPipeline()))
            {
                runners.push_back(source.second->createLogicalPipelineRunner(NULL));
            }

            else if(isRuntimeScalablePipeline(source.second->getLogicalPipeline()))
            {
                for(int i = 0 ; i < this->initial_runtime_scalable_pipeline_concurrent ; i++)
                    runners.push_back(source.second->createLogicalPipelineRunner(NULL));
            }
            else if(isBuildPipeline(source.second->getLogicalPipeline()) && isSourcePipeline(source.second->getLogicalPipeline()))
            {
                ExecutionConfig config;
                int con = atoi(config.getIntra_task_hash_build_concurrency().c_str());
                if((con & con - 1) != 0)
                {
                    spdlog::warn("The value of intra-task hash build concurrency must be an integer power of 2!");
                    con = 1;
                }


                for(int i = 0 ; i < con ; i++)
                    runners.push_back(source.second->createLogicalPipelineRunner(NULL));

            }
            else
                runners.push_back(source.second->createLogicalPipelineRunner(NULL));
        }
        this->taskExecutor->enqueueSplits(this->taskHandle,runners);
    }

    void scheduleRemoteSource(set<shared_ptr<ScheduledSplit>> splits)
    {
        map<PlanNodeId,set<shared_ptr<Split>>> multiRemoteTask;
        map<PlanNodeId,vector<string>> addrs;
        vector<std::shared_ptr<SplitRunner>> runners;

        for(auto split : splits)
        {
            multiRemoteTask[split->getPlanNodeId()].insert(split->getSplit());
            addrs[split->getPlanNodeId()].push_back(static_pointer_cast<RemoteSplit>(split->getSplit()->getConnectorSplit())->getTaskId()->ToString());
        }

        for(auto task : multiRemoteTask) {

            PipelineId pipelineId = this->sourcePlanNodeId_To_LPipeline[task.first];

            list<shared_ptr<DriverContext>> driverContexts = this->taskContext->getPipelineContext(pipelineId)->getDriverContexts();
            if(!driverContexts.empty()) {

                for (auto driverContext: driverContexts) {
                    driverContext->addRuntimeLocation(task.second);
                }
            }
            else
            {
                vector<string> addrss = addrs[task.first];
                string locations;
                for(auto s : addrss)
                {
                    locations+=s+"|";
                }
                spdlog::debug(this->taskId->ToString() + " start get page from ---> "+locations);

                auto pipe = this->remoteSourcePipelineFactory[pipelineId]->getLogicalPipeline();
                if(isRemoteOutputPipeline(pipe) && isRuntimeScalablePipeline(pipe))
                {
                    for(int i = 0 ; i < this->initial_runtime_scalable_pipeline_concurrent ; i++) {
                        std::shared_ptr<SplitRunner> splitRunner = this->remoteSourcePipelineFactory[pipelineId]->createLogicalPipelineRunner(task.second);
                        runners.push_back(splitRunner);
                    }
                }

                else if(isRemoteSinkPipeline(pipe))
                {
                    if(isHashExchangePipeline(pipe))
                    {
                        ExecutionConfig config;
                        int con = atoi(config.getIntra_task_hash_build_concurrency().c_str());
                        if((con & con - 1) != 0)
                        {
                            spdlog::warn("The value of intra-task hash build concurrency must be an integer power of 2!");
                            con = 1;
                        }
                        for(int i = 0 ; i < con ; i++)
                            runners.push_back(this->remoteSourcePipelineFactory[pipelineId]->createLogicalPipelineRunner(task.second));
                    }
                    else
                        runners.push_back(this->remoteSourcePipelineFactory[pipelineId]->createLogicalPipelineRunner(task.second));
                }

                else
                {
                    std::shared_ptr<SplitRunner> splitRunner = this->remoteSourcePipelineFactory[pipelineId]->createLogicalPipelineRunner(task.second);
                    runners.push_back(splitRunner);
                }

    //            auto id = this->remoteSourcePipelineFactory[pipelineId]->getLogicalPipeline()->getLogicalPipelines().back()->getTypeId();
     //           if(id == "Logical_LocalExchangeSinkOperator"){
     //               std::shared_ptr<SplitRunner> splitRunner = this->remoteSourcePipelineFactory[pipelineId]->createLogicalPipelineRunner(task.second);
      //              runners.push_back(splitRunner);
      //          }


            }
        }

        this->taskExecutor->enqueueSplits(this->taskHandle,runners);

    }



    void startRemoteTask(set<shared_ptr<ScheduledSplit>> splits)
    {
        scheduleRemoteSource(splits);
    }
    void startTableScanTask(set<shared_ptr<ScheduledSplit>> splits)
    {

        vector<std::shared_ptr<SplitRunner>> runners;
        for(auto split : splits) {

            PipelineId pipelineId = this->sourcePlanNodeId_To_LPipeline[split->getPlanNodeId()];
            runners.push_back(this->tableScanPipelineFactory[pipelineId]->createLogicalPipelineRunner(split));

        }
        this->taskExecutor->enqueueSplits(this->taskHandle,runners);

    }

    void updateSources(shared_ptr<TaskSource> taskSource)
    {

        this->taskStateMachine->start();
        //we can use planNodeId to figure out what the split for, tableScanPipeline or remoteSourcePipeline



        //we can get plannodeId from tasksource,figure out this task source's plannodeId.

        //if a fragment has mul sources,how to tell task source,which source is you to set? we can use plannode id to identify.


        set<shared_ptr<ScheduledSplit>> tableScanSplits;
        set<shared_ptr<ScheduledSplit>> remoteSplits;

        for(auto split : taskSource->getSplits())
        {
            PlanNodeId planNodeId = split->getPlanNodeId();
            if(this->sourcePlanNodeId_To_LPipeline.find(planNodeId) != this->sourcePlanNodeId_To_LPipeline.end()) {
                if(planNodeId.get().find("RemoteSource") == string::npos)
                    tableScanSplits.insert(split);
                else
                    remoteSplits.insert(split);
            }
        }

        if(!tableScanSplits.empty())
            startTableScanTask(tableScanSplits);
        if(!remoteSplits.empty())
            startRemoteTask(remoteSplits);
        /*
        if(this->sourcePlanNodeId_To_LPipeline.find(planNodeId) != this->sourcePlanNodeId_To_LPipeline.end()) {
            if(planNodeId.get().find("RemoteSource") == string::npos)
                startTableScanTask(taskSource->getSplits());
            else
                startRemoteTask(taskSource->getSplits());
        }
        else
            spdlog::critical("Runtime ERROR! Cannot find the runnerFactory by the planNodeId"+planNodeId.get());
       */
       // test(this);
    }


    static void tt(SqlTaskExecution *a)
    {

        sleep(2);
        a->increaseARemoteSourceCPUPipeLine(PipelineId("0"));

        a->increaseARemoteSourceCPUPipeLine(PipelineId("0"));
        a->increaseARemoteSourceCPUPipeLine(PipelineId("0"));


        sleep(2);
        a->closeARemoteSourceCPUPipeLine(PipelineId("0"));
    }
    static void test(SqlTaskExecution *a)
    {

        thread cc(&tt,a);
       cc.detach();
    }



    void increaseASourceCPUPipeLine(PipelineId pipelineName)
    {
        spdlog::debug("increaseASourceCPUPipeLine in");
        if (sourceLogicalPipelineRegister.find(pipelineName) == sourceLogicalPipelineRegister.end())
        {
            spdlog::critical("Cannot find the Source Pipeline '"+pipelineName.get()+"'");
            return;
        }

        auto factory = this->sourcePipelineFactory[pipelineName];
        vector<std::shared_ptr<SplitRunner>> runners;

        runners.push_back(factory->createLogicalPipelineRunner(NULL));
        spdlog::debug("increaseASourceCPUPipeLine creating runners");

        this->taskExecutor->enqueueSplits(this->taskHandle,runners);
        spdlog::debug("increaseASourceCPUPipeLine add runners to execute");

    }

    void increaseARemoteSourceCPUPipeLine(PipelineId pipelineName)
    {
        spdlog::debug("increaseARemoteSourceCPUPipeLine in");
        if (remoteSourceLogicalPipelineRegister.find(pipelineName) == remoteSourceLogicalPipelineRegister.end())
        {
            spdlog::critical("Cannot find the Source Pipeline '"+pipelineName.get()+"'");
            return;
        }

        auto factory = this->remoteSourcePipelineFactory[pipelineName];
        vector<std::shared_ptr<SplitRunner>> runners;

   //     if(factory->getLogicalPipeline()->getLogicalPipelines().size() != 5)
  //      return;
     //   if(factory->getLogicalPipeline()->getLogicalPipelines().back()->getTypeId() != "Logical_LocalExchangeSinkOperator")
       // {
         //   return;
        //}

        auto splits = this->taskContext->getPipelineContext(pipelineName)->getAllRegRemoteSplit();
        if(splits.size() == 0)
        {
            spdlog::debug("No remote splits found! RemoteSourcePipeline cannot be created!");
            return;
        }

        runners.push_back(factory->createLogicalPipelineRunner(splits));
        spdlog::debug("increaseARemoteSourceCPUPipeLine creating runners");

        this->taskExecutor->enqueueSplits(this->taskHandle,runners);
        spdlog::debug("increaseARemoteSourceCPUPipeLine add runners to execute");

    }

    void increasePipelineDriver(PipelineId pipelineName) {
        if (sourceLogicalPipelineRegister.count(pipelineName) > 0) {
            this->increaseASourceCPUPipeLine(pipelineName);
        } else if (remoteSourceLogicalPipelineRegister.count(pipelineName) > 0) {
            this->increaseARemoteSourceCPUPipeLine(pipelineName);
        }
    }

    void closePipelineDriver(PipelineId pipelineName) {
        if (sourceLogicalPipelineRegister.count(pipelineName) > 0) {
            this->closeASourceCPUPipeline(pipelineName);
        } else if (remoteSourceLogicalPipelineRegister.count(pipelineName) > 0) {
            this->closeARemoteSourceCPUPipeLine(pipelineName);
        }
    }

    void closeASourceCPUPipeline(PipelineId pipelineName)
    {

        if (sourceLogicalPipelineRegister.find(pipelineName) == sourceLogicalPipelineRegister.end())
        {
            spdlog::critical("Cannot find the Source Pipeline '"+pipelineName.get()+"'");
            return;
        }

        auto factory = this->localExchanges[pipelineName.get()];
        factory->getLocalExchange()->closeSource(1);

    }
    void closeARemoteSourceCPUPipeLine(PipelineId pipelineName)
    {
        if (remoteSourceLogicalPipelineRegister.find(pipelineName) == remoteSourceLogicalPipelineRegister.end())
        {
            spdlog::critical("Cannot find the RemoteSource Pipeline '"+pipelineName.get()+"'");
            return;
        }

        auto pipelineContext = this->taskContext->getPipelineContext(pipelineName);
        if(pipelineContext == NULL)
            return;
        list<shared_ptr<DriverContext>> driverContexts = pipelineContext->getDriverContexts();
        if(driverContexts.size() > 1) {

            for (auto driverContext: driverContexts) {
                driverContext->closeRemoteSourceDriver();
                break;
            }
        }

    }
    void increaseAllSourcePipelineForTask(int num)
    {
        for(auto logicalPipeline : this->sourcePipelineFactory) {

            /*
            vector<std::shared_ptr<SplitRunner>> runners;

            for(int i = 0 ; i < num ; i++) {
                runners.push_back(logicalPipeline.second->createLogicalPipelineRunner(NULL));
                spdlog::debug("increaseAllSourcePipelineForTask creating runners");
            }

            this->taskExecutor->enqueueSplits(this->taskHandle, runners);
            spdlog::debug("increaseAllSourcePipelineForTask add runners to execute");
             */
            if(isRuntimeScalablePipeline(logicalPipeline.second->getLogicalPipeline())) {
                for (int i = 0; i < num; i++) {
                    increaseASourceCPUPipeLine(logicalPipeline.first);

                }
            }

        }
    }

    void increaseAllScalablePipelineForTask(int num)
    {
        for(auto logicalPipeline : this->sourcePipelineFactory) {

            if(isRuntimeScalablePipeline(logicalPipeline.second->getLogicalPipeline())) {
                for (int i = 0; i < num; i++) {
                    increaseASourceCPUPipeLine(logicalPipeline.first);
                }
            }

        }
        for(auto logicalPipeline : this->remoteSourcePipelineFactory) {

            if(isRuntimeScalablePipeline(logicalPipeline.second->getLogicalPipeline())) {
                for (int i = 0; i < num; i++) {
                    increaseARemoteSourceCPUPipeLine(logicalPipeline.first);
                }
            }

        }


    }


    //----------------------------------------------------------------------------------------------------------------------//


    vector<shared_ptr<PipelineDescriptor>> getPipelineDescs(map<PipelineId, pair<std::shared_ptr<LogicalPipeline>, int>> pipelineRegister)
    {
        vector<shared_ptr<PipelineDescriptor>> pipelineDescriptors;

        for(auto pipelines : pipelineRegister)
        {
            PipelineId pId = pipelines.first;
            vector<string> pTems = pipelines.second.first->getPipelineOperatorTypes();
            int driverCount = this->taskContext->getPipelineContext(pipelines.first)->getRunningDriverCount();
            string pipelineType = pipelines.second.first->getPipelineTypesString();

            list<shared_ptr<DriverContext>> driverCons = this->taskContext->getPipelineContext(pipelines.first)->getDriverContexts();
            shared_ptr<TableScanRecord> tableScanRecord = NULL;
            for(auto driverCon : driverCons)
            {
                if(driverCon->getTableScanRecord() != NULL) {
                    tableScanRecord = driverCon->getTableScanRecord();
                    break;
                }
            }
            bool scalable = isRuntimeScalablePipeline(pipelines.second.first);
            if(tableScanRecord == NULL)
                pipelineDescriptors.push_back(make_shared<PipelineDescriptor>(pId.get(),pipelineType,driverCount,pTems,scalable));
            else
                pipelineDescriptors.push_back(make_shared<PipelineDescriptor>(pId.get(),pipelineType,driverCount,pTems,tableScanRecord,scalable));
        }
        return pipelineDescriptors;
    }

    vector<shared_ptr<PipelineDescriptor>> getPipelineDescriptors()
    {
        vector<shared_ptr<PipelineDescriptor>> allDescs;
        vector<shared_ptr<PipelineDescriptor>> tempDescs;
        tempDescs = getPipelineDescs(this->tableScanLogicalPipelineRegister);
        allDescs.insert(allDescs.end(),tempDescs.begin(),tempDescs.end());
        tempDescs = getPipelineDescs(this->remoteSourceLogicalPipelineRegister);
        allDescs.insert(allDescs.end(),tempDescs.begin(),tempDescs.end());
        tempDescs = getPipelineDescs(this->sourceLogicalPipelineRegister);
        allDescs.insert(allDescs.end(),tempDescs.begin(),tempDescs.end());
        return allDescs;
    }



};

#endif //OLVP_SQLTASKEXECUTION_HPP
