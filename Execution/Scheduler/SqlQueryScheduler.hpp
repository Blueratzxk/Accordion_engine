//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SQLQUERYSCHEDULER_HPP
#define OLVP_SQLQUERYSCHEDULER_HPP


#include "StageTreeExecutionFactory.hpp"
#include "NormalStageScheduler.hpp"
#include "../Task/Fetcher/TaskResultFetcher.hpp"
#include "../../Utils/Timer.hpp"

#include <stdio.h>
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>


#include "../../Query/QueryStateMachine.hpp"
#include "QueryInfos/StageProcessingTimeCollector.hpp"


class SqlQueryScheduler : public enable_shared_from_this<SqlQueryScheduler>{

    PlanNode *rawTree;
    shared_ptr<SubPlan> root;
    atomic<bool> isStarted = false;

    shared_ptr<StageTreeExecutionFactory> executionFactory;

    shared_ptr<Session> session;
    map<int, shared_ptr<StageExecutionAndScheduler>> stageExecutionsMap;
    map<int,double> stageExecutionTimePredictionAfterTuned;

    vector<StageExecutionAndScheduler> stageExeSchedulers;
    shared_ptr<SqlStageExecution> rootStage = NULL;

    shared_ptr<QueryStateMachine> stateMachine = NULL;
    list<shared_ptr<DataPage>> resultSet;

    atomic<bool> IQRS_Signal = false;

    double originTime = 0.0;
    double actualTime = 0.0;
    double executionTime = 0.0;
    shared_ptr<StageProcessingTimeCollector> stageProcessingTimeCollector;

public:
    SqlQueryScheduler(PlanNode * rawTree,shared_ptr<SubPlan> tree,shared_ptr<Session> session,shared_ptr<QueryStateMachine> stateMachine)
    {
        this->root = tree;
        this->rawTree = rawTree;
        this->session = session;
        this->stateMachine = stateMachine;
        this->executionFactory = make_shared<StageTreeExecutionFactory>(this->session);

        this->stageExeSchedulers = this->executionFactory->createStageTreeExecutions(this->rootStage, this->root);

        for (auto stageExecution: stageExeSchedulers)
        {
            stageExecutionsMap[stageExecution.getStageExecution()->getStageId().getId()] = make_shared<StageExecutionAndScheduler>(stageExecution.getStageExecution(),
                                                                                                                       stageExecution.getStageLinkage(),stageExecution.getStageScheduler());
        }

        list<shared_ptr<SqlStageExecution>> stageExecutions;
        for(auto exe : this->stageExeSchedulers)
            stageExecutions.push_back(exe.getStageExecution());

        this->stageProcessingTimeCollector = make_shared<StageProcessingTimeCollector>(stageExecutions);
        this->stageProcessingTimeCollector->start();
    }



    double getOriginExecutionTime()
    {
        return this->originTime;
    }
    double getActualExecutionTime()
    {
        return this->actualTime;
    }
    double getExecutionTime()
    {
        return this->executionTime;
    }

    vector<StageExecutionAndScheduler> getStageExeSchedulers()
    {
        return this->stageExeSchedulers;
    }

    bool isRootStage(shared_ptr<SqlStageExecution> stage)
    {
        return stage == this->rootStage;
    }
    bool canIQRS()
    {
        return this->IQRS_Signal;
    }
    void openIQRS()
    {
        this->IQRS_Signal = true;
    }

    shared_ptr<map<int,shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>>>> getStagesNodeTaskMap()
    {
        shared_ptr<map<int,shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>>>> result =
                make_shared<map<int,shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>>>>();

        for(auto stage : this->stageExeSchedulers)
        {
            auto re = stage.getStageExecution()->getActiveTaskNodeMap();
            (*result)[stage.getStageExecution()->getStageId().getId()] = re;
        }
        return result;

    }

    void setStageFirstExecutionTimePrediction(shared_ptr<StageExecutionAndScheduler> stage)
    {
        if(stageExecutionTimePredictionAfterTuned.count(stage->getStageExecution()->getStageId().getId()) > 0)
            return;
        auto tablescan = findRootTableScanStageForStage(to_string(stage->getStageExecution()->getStageId().getId()));

        double time = tablescan->getStageExecution()->getRemainingTime() + tablescan->getStageExecution()->getState()->getRunningTime();
        stageExecutionTimePredictionAfterTuned[stage->getStageExecution()->getStageId().getId()] = time;
    }

    shared_ptr<StageExecutionAndScheduler> getStageExecutionAndSchedulerByStagId(int stageId)
    {
        if(this->stageExecutionsMap.count(stageId) > 0)
            return this->stageExecutionsMap[stageId];

        return NULL;
    }


    //------------------------------------------------------------------------------//

    shared_ptr<SqlStageExecution> findStageExecutionByTableScanId(string tableScanId){

        for(auto stageScheduler : this->stageExeSchedulers)
        {
            if(stageScheduler.getStageExecution()->getFragment()->hasTableScanId(tableScanId))
                return stageScheduler.getStageExecution();
        }
        return NULL;
    }

    string traversePlanToFindJoinBuildSideTableScanId(PlanNode *node) {

        if(node->getType() == "TableScanNode") {
            return node->getId();
        }

        auto childs = node->getSources();
        if(!childs.empty())
            return traversePlanToFindJoinBuildSideTableScanId(childs[0]);

        return "0";
    }

    void traversePlanToFindJoin(PlanNode *root)
    {
        if(root->getType() == "LookupJoinNode" || root->getType() == "CrossJoinNode") {

            string tablescanId;
            if(root->getType() == "LookupJoinNode")
            {
                tablescanId = traversePlanToFindJoinBuildSideTableScanId(((LookupJoinNode*)root)->getBuild());
            }
            else if (root->getType() == "CrossJoinNode") {
                tablescanId = traversePlanToFindJoinBuildSideTableScanId(((CrossJoinNode *) root)->getBuild());
            }
            spdlog::info(root->getId()+"----"+ to_string(findStageExecutionByTableScanId(tablescanId)->getStageId().getId()));

        }

        auto childs = root->getSources();

        for(auto child : childs)
            traversePlanToFindJoin(child);

    }

    map<string,shared_ptr<SqlStageExecution>> getJoinIdToTableScanStage()
    {
        map<string,shared_ptr<SqlStageExecution>> result;
        auto planRoot = this->rawTree;
        traversePlanToFindJoin(planRoot);

        return result;
    }

    //------------------------------------------------------------------------------//

    shared_ptr<StageExecutionAndScheduler> findRootTableScanStageForStage(string stageId)
    {


        shared_ptr<StageExecutionAndScheduler> es = this->getStageExecutionAndSchedulerByStagId(atoi(stageId.c_str()));

        while(!es->getStageLinkage()->getChildStages().empty())
        {
            auto stageExe = es->getStageLinkage()->getChildStages()[0];
            es = getStageExecutionAndSchedulerByStagId(stageExe->getStageId().getId());
        }
        return es;

    }

    map<int,long> getStageBuildChildsTotalBytes(string stageId)
    {
        vector<shared_ptr<SqlStageExecution>> result;
        shared_ptr<StageExecutionAndScheduler> es = this->getStageExecutionAndSchedulerByStagId(atoi(stageId.c_str()));

        map<int,long> stageBytes;

        long all = 0;
        if(es->getStageLinkage()->getChildStages().size() > 1)
        {
            auto stageExe = es->getStageLinkage()->getChildStages();
            for(int i = 1 ; i < stageExe.size() ; i++) {
                result.push_back(stageExe[i]);
                stageBytes[stageExe[i]->getStageId().getId()] = stageExe[i]-> getTotalBytesTasksOutput();
            }
        }
        return stageBytes;
    }

    vector<shared_ptr<StageExecutionAndScheduler>> getLeftChildStagesForStage(string stageId)
    {

        vector<shared_ptr<StageExecutionAndScheduler>> result;
        shared_ptr<StageExecutionAndScheduler> es = this->getStageExecutionAndSchedulerByStagId(atoi(stageId.c_str()));

        while(!es->getStageLinkage()->getChildStages().empty())
        {
            auto stageExe = es->getStageLinkage()->getChildStages()[0];
            es = getStageExecutionAndSchedulerByStagId(stageExe->getStageId().getId());
            result.push_back(es);
        }
        return result;

    }

    set<shared_ptr<StageExecutionAndScheduler>> getLeftChildStagesSetForStage(string stageId)
    {

        set<shared_ptr<StageExecutionAndScheduler>> result;
        shared_ptr<StageExecutionAndScheduler> es = this->getStageExecutionAndSchedulerByStagId(atoi(stageId.c_str()));

        while(!es->getStageLinkage()->getChildStages().empty())
        {
            auto stageExe = es->getStageLinkage()->getChildStages()[0];
            es = getStageExecutionAndSchedulerByStagId(stageExe->getStageId().getId());
            result.insert(es);
        }
        return result;

    }



    double getSingleTaskOfStageCpuUsage(int stageId)
    {
        return this->stageExecutionsMap[stageId]->getStageExecution()->getSingleTaskOfTheStageCpuUsage();
    }

    double getStageRemainingCpuUsageRatioByTaskThreadNums(int stageId)
    {
        return this->stageExecutionsMap[stageId]->getStageExecution()->getRemainingCpuUsageRatioOfStageByTaskThreadNums();
    }


    map<int,list<taskCpuNetUsageInfo>> getStagesCpuUsages()
    {
        map<int,list<taskCpuNetUsageInfo>> stagesCpuUsages;

        for(auto stage : this->stageExeSchedulers)
        {
            auto usages = stage.getStageExecution()->getStageCpuUsages();
            stagesCpuUsages[stage.getStageExecution()->getStageId().getId()] = usages;
        }

        return stagesCpuUsages;
    }

    vector<string> getStagesInfo()
    {

        vector<string> stagesInfo;
        for(auto execution : this->stageExecutionsMap)
        {
            stagesInfo.push_back(execution.second->getStageExecution()->getStageInfo());
        }
        return stagesInfo;

    }

    map<int,map<int,string>> getStagesBuildRecords()
    {

        map<int,map<int,string>> stagesBuildRecords;
        for(auto execution : this->stageExeSchedulers)
        {
            stagesBuildRecords[execution.getStageExecution()->getStageId().getId()] = execution.getStageExecution()->getBuildRecords();
        }
        return stagesBuildRecords;

    }

    string getRootStageExecutionTime()
    {
        string info = this->rootStage->getStageInfo();
        nlohmann::json j = nlohmann::json::parse(info);
        return j["taskInfos"][0]["RunningTime"];
    }

    void cleanEmptyResult()
    {
        for (list<shared_ptr<DataPage>>::iterator it = this->resultSet.begin(); it != this->resultSet.end();)
        {
            if (it->get()->isEmptyPage()|| it->get()->isEndPage())
                this->resultSet.erase(it++);
            else
                ++it;
        }

    }

    list<shared_ptr<DataPage>> getResultSet()
    {
        cleanEmptyResult();
        return this->resultSet;
    }



    void addConcurrencyForOneStage(vector<StageExecutionAndScheduler> executions, int stageId) {

        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {
                ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(executions[i].getStageScheduler()))->addOneConcurrent();
                vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();
                executions[i].getStageLinkage()->processScheduleResultsToAddOneLocationDynamically(newTasks);

                this->setStageFirstExecutionTimePrediction(make_shared<StageExecutionAndScheduler>(executions[i].getStageExecution(),
                                                                                                   executions[i].getStageLinkage(),executions[i].getStageScheduler()));
            }

        }

    }
    void addMulConcurrencyForOneStage(vector<StageExecutionAndScheduler> executions, int stageId,int taskNums)
    {
        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {

                ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(executions[i].getStageScheduler()))->addMulConcurrent(taskNums);
                vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();
                executions[i].getStageLinkage()->processScheduleResultsToAddConcurrent(newTasks);

                this->setStageFirstExecutionTimePrediction(make_shared<StageExecutionAndScheduler>(executions[i].getStageExecution(),
                                                                                                   executions[i].getStageLinkage(),executions[i].getStageScheduler()));


            }

        }
    }

    void addMulConcurrencyForOneStageByNodesGroup(vector<StageExecutionAndScheduler> executions, int stageId,int taskNums)
    {
        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {

                ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(executions[i].getStageScheduler()))->addMulConcurrent(1,taskNums);
                vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();
                executions[i].getStageLinkage()->processScheduleResultsToAddConcurrent(newTasks);

                this->setStageFirstExecutionTimePrediction(make_shared<StageExecutionAndScheduler>(executions[i].getStageExecution(),
                                                                                                   executions[i].getStageLinkage(),executions[i].getStageScheduler()));


            }

        }
    }


    void decreaseParallelismForOneStage(vector<StageExecutionAndScheduler> executions, int stageId)
    {
        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {
                shared_ptr<SqlStageExecution> stageExe = executions[i].getStageExecution();
                (static_pointer_cast<NormalStageScheduler>(executions[i].getStageScheduler()))->decreaseOneConcurrentBySourceStage();
            }

        }

    }
    void decreaseTaskGroupParallielismForOneStage(vector<StageExecutionAndScheduler> executions, int stageId)
    {
        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {
                shared_ptr<SqlStageExecution> stageExe = executions[i].getStageExecution();
                (static_pointer_cast<NormalStageScheduler>(executions[i].getStageScheduler()))->decreaseTaskGroup();
            }

        }

    }



    void updateIntraTaskParallelism(vector<StageExecutionAndScheduler> executions, int stageId,shared_ptr<TaskIntraParaUpdateRequest> request)
    {
        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == (stageId)) {
                executions[i].getStageExecution()->updateTasksIntraPara(request);

                this->setStageFirstExecutionTimePrediction(make_shared<StageExecutionAndScheduler>(executions[i].getStageExecution(),
                                                                                                   executions[i].getStageLinkage(),executions[i].getStageScheduler()));

            }
        }
    }



    static void addStageConcurrent(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        scheduler->addConcurrencyForOneStage(scheduler->stageExeSchedulers,stageId);
    }

    static void addStageMulConcurrent(shared_ptr<SqlQueryScheduler> scheduler,int stageId,int taskNum)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        scheduler->addMulConcurrencyForOneStage(scheduler->stageExeSchedulers,stageId,taskNum);
    }

    static void addStageMulConcurrentByNodesGroup(shared_ptr<SqlQueryScheduler> scheduler,int stageId,int taskNum)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        scheduler->addMulConcurrencyForOneStageByNodesGroup(scheduler->stageExeSchedulers,stageId,taskNum);
    }

    static void decreaseStageParallelism(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        scheduler->decreaseParallelismForOneStage(scheduler->stageExeSchedulers,stageId);
    }

    static void decreaseStageTaskGroupParallelism(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        scheduler->decreaseTaskGroupParallielismForOneStage(scheduler->stageExeSchedulers,stageId);
    }

    static bool isStageScalable(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {


        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return false;

        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;
        for (int i = 0; i < executions.size(); i++) {
            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {
                return executions[i].getStageExecution()->isStageScalable();
            }
        }

        return false;
    }

    static bool isStageExist(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {
        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;
        for (int i = 0; i < executions.size(); i++) {
            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {
                return true;
            }
        }
        return false;
    }

    static bool isStageHasBuild(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {
        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;
        for (int i = 0; i < executions.size(); i++) {
            auto sta = executions[i].getStageExecution();
            if (sta->getStageId().getId() == stageId) {
            }
        }
        return false;
    }

    static void addQueryConcurrency(shared_ptr<SqlQueryScheduler> scheduler,string degree)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;

        for (int i = 0; i < executions.size(); i++) {
            auto handle = executions[i].getStageExecution()->getFragment()->getPartitionHandle();
            if(handle != NULL && handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0) {

                if(static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType !=
                        SystemPartitioningHandle::SINGLE) {

                    if(atoi(degree.c_str()) != -1)
                    {
                        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>("-1","incre",
                                                                                                                       degree);
                        scheduler->updateIntraTaskParallelism(executions,executions[i].getStageExecution()->getStageId().getId(),intraRequest);
                    }

                }
            }
        }
    }

    static void addStageConcurrency(shared_ptr<SqlQueryScheduler> scheduler,string degree,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;

        int flag = false;
        int index = 0;
        for (int i = 0; i < executions.size(); i++) {
            if(executions[i].getStageExecution()->getStageId().getId() == stageId) {
                flag = true;
                index = i;
            }
        }
        if(!flag)
            return;


        auto handle = executions[index].getStageExecution()->getFragment()->getPartitionHandle();
        if(handle != NULL && handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0) {

            if(static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType !=
               SystemPartitioningHandle::SINGLE) {

                if(atoi(degree.c_str()) != -1)
                {
                    shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>("-1","incre",
                                                                                                                   degree);
                    scheduler->updateIntraTaskParallelism(executions,executions[index].getStageExecution()->getStageId().getId(),intraRequest);
                }
            }
        }


    }

    static void addQueryParallelism(shared_ptr<SqlQueryScheduler> scheduler,string degree)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;

        for (int i = 0; i < executions.size(); i++) {
            auto handle = executions[i].getStageExecution()->getFragment()->getPartitionHandle();
            if (handle != NULL &&
                handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0) {
                if (static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType !=
                    SystemPartitioningHandle::SINGLE) {

                    int addConcur = atoi(degree.c_str());
                    if (addConcur != -1)
                        scheduler->addMulConcurrencyForOneStage(executions,
                                                                executions[i].getStageExecution()->getStageId().getId(),
                                                                addConcur);
                }
            }
        }
    }

    static void addQueryParallelismUsingInitialNodes(shared_ptr<SqlQueryScheduler> scheduler,string degree)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;

        for (int i = 0; i < executions.size(); i++) {
            auto handle = executions[i].getStageExecution()->getFragment()->getPartitionHandle();
            if (handle != NULL &&
                handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0) {
                if (static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType !=
                    SystemPartitioningHandle::SINGLE) {

                    int addConcur = atoi(degree.c_str());
                    if (addConcur != -1)
                        scheduler->addMulConcurrencyForOneStageByNodesGroup(executions,
                                                                executions[i].getStageExecution()->getStageId().getId(),
                                                                addConcur);
                }
            }
        }
    }


    static void addStageAllTaskIntraPipelineConcurrent(shared_ptr<SqlQueryScheduler> scheduler,string stageId,string pipelineId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>(pipelineId,"incre","1");
        scheduler->updateIntraTaskParallelism(scheduler->stageExeSchedulers,atoi(stageId.c_str()),intraRequest);
    }

    static void closeStageAllTaskIntraPipelineConcurrent(shared_ptr<SqlQueryScheduler> scheduler,string stageId,string pipelineId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>(pipelineId,"decre","1");
        scheduler->updateIntraTaskParallelism(scheduler->stageExeSchedulers,atoi(stageId.c_str()),intraRequest);
    }

    static std::string convertDoubleToString(const long double value,const int precision = 0)
    {
        std::stringstream stream{};
        stream<<std::fixed<<std::setprecision(precision)<<value;
        return stream.str();
    }


    static string getQueryStagesThroughputs(shared_ptr<SqlQueryScheduler> scheduler)
    {
        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;
        nlohmann::json throughputs;
        for(auto exe : executions)
        {



            double through = exe.getStageExecution()->getStageThroughput();

            double remainingTime = exe.getStageExecution()->getRemainingTime();

            if(exe.getStageExecution()->getSourceTasks().size() > 0)
                remainingTime = exe.getStageLinkage()->getChildStages()[0]->getNonTableScanRemainingTime();

            remainingTime = remainingTime/1000;



            throughputs[to_string(exe.getStageExecution()->getStageId().getId())] = convertDoubleToString(through,3);
            throughputs[to_string(exe.getStageExecution()->getStageId().getId())+"_remain"] = convertDoubleToString(remainingTime,4);
        }
        string result = throughputs.dump();
        return result;
    }



    static string getQueryStagesThroughputsInfo(shared_ptr<SqlQueryScheduler> scheduler)
    {
        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;
        nlohmann::json throughputs;
        for(auto exe : executions)
        {
            string through = exe.getStageExecution()->getStageThroughputInfo();
            throughputs[to_string(exe.getStageExecution()->getStageId().getId())] = (through);
        }
        string result = throughputs.dump();
        return result;
    }

    static void schedule(shared_ptr<SqlQueryScheduler> scheduler) {

        spdlog::debug(scheduler->session->getQueryId() +" starts initial schedule!");
        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;


        for (int i = 0; i < executions.size(); i++) {
            if(executions[i].getStageScheduler() == NULL)
            {
                spdlog::critical("Scheduler failed ! Some scheduler pointer is NULL!");

                scheduler->stateMachine->canceled();
                return;
            }
        }

        for (int i = 0; i < executions.size(); i++) {
            executions[i].getStageExecution()->beginScheduling();
            spdlog::debug("Schedule "+executions[i].getStageScheduler()->getSchedulerType()+" stageId:"+ to_string(executions[i].getStageExecution()->getStageId().getId()));
            ScheduleResult result = executions[i].getStageScheduler()->schedule();
            vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();
            spdlog::debug("Pre Schedule OK, process schedule results");
            executions[i].getStageLinkage()->processScheduleResults(newTasks);
            spdlog::debug("Schedule "+executions[i].getStageScheduler()->getSchedulerType()+" stageId:"+ to_string(executions[i].getStageExecution()->getStageId().getId())+" OK!");

        }


        scheduler->stateMachine->start();


 //--------------------------------System Add Concurrent--------------------------------------//
        ExecutionConfig config;
        string tsaisc = config.getTEST_system_add_intra_stage_concurrency();
        string tsaitc = config.getTEST_system_add_intra_task_concurrency();
        string tsatgc = config.getTEST_system_add_task_group_concurrency();
        string partitionCount = config.getInitial_hash_partition_concurrency();

        int intraTaskConcur = atoi(tsaitc.c_str());


        for (int i = 0; i < executions.size(); i++) {
            auto handle = executions[i].getStageExecution()->getFragment()->getPartitionHandle();
            if(handle != NULL && handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0) {
                if (static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType ==
                    SystemPartitioningHandle::HASH_SCALED) {

                    int tgc = atoi(tsatgc.c_str());
                    if(tgc!= -1) {
                        for (int j = 0; j < tgc; j++) {
                                scheduler->addMulConcurrencyForOneStage(executions,
                                                                        executions[i].getStageExecution()->getStageId().getId(),
                                                                        atoi(partitionCount.c_str()));
                        }
                    }

                }
                else if(static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType !=
                        SystemPartitioningHandle::SINGLE) {

                    int addConcur = atoi(tsaisc.c_str());
                    if(addConcur != -1)
                        scheduler->addMulConcurrencyForOneStage(executions,executions[i].getStageExecution()->getStageId().getId(),addConcur);

                    if(intraTaskConcur != -1)
                    {
                        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>("-1","incre",
                                                                                                                       to_string(intraTaskConcur));
                        scheduler->updateIntraTaskParallelism(executions,executions[i].getStageExecution()->getStageId().getId(),intraRequest);
                    }

                }
            }
        }
//------------------------------------System Add Concurrent--------------------------------------------//

        vector<shared_ptr<HttpRemoteTask>> remoteTasks = scheduler->rootStage->getAllTasks();
        shared_ptr<TaskResultFetcher> taskResultFetcher = make_shared<TaskResultFetcher>(remoteTasks[0]->getTaskId(),remoteTasks[0]->getIP(),"9081","0");

        shared_ptr<DataPage> result;

        scheduler->openIQRS();


        spdlog::debug(scheduler->session->getQueryId() +" initial schedule finished!");

        Timer timer;
        bool infoTag = true;



        for(;;)
        {

            int counter = 0;

            /*
            timer.set();
            if(timer.checkGap(300)) {
                for(auto exe : executions)
                {
                    double through = exe.getStageExecution()->getStageThroughput();
                    spdlog::debug(
                            "Throughput_Stage_" + to_string(exe.getStageExecution()->getStageId().getId()) + "====>" +
                            to_string(through));
                }
            }*/



            for(auto exe : executions)
            {
                if(exe.getStageExecution()->getState()->isDone())
                    counter++;
                else
                    exe.getStageExecution()->getStateChangeListener()->listen();
            }
            if(counter == executions.size())
            {
                if(infoTag == true) {
                    spdlog::info("Query finished!");
                    infoTag = false;
                }
                scheduler->stateMachine->finished();

            }


            if(scheduler->stateMachine->isFinished()) {
                taskResultFetcher->schedule();
                result = taskResultFetcher->pollPage();

                if (result != NULL && !result->isEndPage() && result->getElementsCount() > 0) {
                    scheduler->resultSet.push_back(result);
                }

                if (result != NULL && !result->isEndPage()) {
                    ArrowRecordBatchViewer::PrintBatchRows(result->get());
                }

                if (result != NULL && result->isEndPage()) {
                    scheduler->resultSet.push_back(result);

                    break;
                }
            }

        }


        double originTime = 0.0;
        double actualTime = 0.0;
        double exeTime = 0.0;
        for(auto exe : executions)
        {
            exe.getStageExecution()->closeAllTasks();

            if(exe.getStageExecution()->getState()->getRunningTime() > exeTime)
                exeTime = exe.getStageExecution()->getState()->getRunningTime();

            int stageId = exe.getStageExecution()->getStageId().getId();
            if(scheduler->stageExecutionTimePredictionAfterTuned.count(stageId) > 0)
            {
                double origin = scheduler->stageExecutionTimePredictionAfterTuned[stageId];
                if(origin > 0)
                    originTime += origin;
                else
                    originTime += exe.getStageExecution()->getState()->getRunningTime();

                actualTime += exe.getStageExecution()->getState()->getRunningTime();
            }

            spdlog::info( to_string(exe.getStageExecution()->getMaxHashTableBuildTimeofTasks()));

            auto jbs = exe.getStageExecution()->getJoinIdToBuildTime();
            nlohmann::json json;
            json["joinToBuildTime"] = jbs;
            spdlog::info(json.dump());
        }

        scheduler->originTime = originTime;
        scheduler->actualTime = actualTime;
        scheduler->executionTime = exeTime;

           vector<string> results = scheduler->getStagesInfo();
        for(auto re : results)
        {
            spdlog::info(re);
        }

        auto processingTimes = scheduler->stageProcessingTimeCollector->getStageProcessingTimes();
        for(auto pt : processingTimes)
            spdlog::info("Stage "+to_string(pt.first->getStageId().getId())+": "+ to_string(pt.second)+" ms");



        scheduler->getJoinIdToTableScanStage();

        if(originTime > actualTime)
            spdlog::info("Origin:"+to_string(originTime)+" Actual:"+to_string(actualTime)+" Saved "+ to_string((originTime-actualTime)/originTime) +"% percent of time!");
        else
            spdlog::info("Execution time:"+to_string(scheduler->getExecutionTime()));

    }



};


#endif //OLVP_SQLQUERYSCHEDULER_HPP
