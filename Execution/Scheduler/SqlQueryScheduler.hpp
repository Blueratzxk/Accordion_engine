//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SQLQUERYSCHEDULER_HPP
#define OLVP_SQLQUERYSCHEDULER_HPP


#include "StageTreeExecutionFactory.hpp"
#include "NormalStageScheduler.hpp"

#include "../../Utils/Timer.hpp"

#include <stdio.h>
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>


#include "../../Query/QueryStateMachine.hpp"
#include "QueryInfos/StageProcessingTimeCollector.hpp"

#include "SidewayExchangeSystem/SidewayExchangeSystem.hpp"


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

    shared_ptr<SidewayExchangeSystem> sidewayExchangeSystem;
    shared_ptr<SqlScheduleLock> scheduleLock;



public:
    SqlQueryScheduler(PlanNode * rawTree,shared_ptr<SubPlan> tree,shared_ptr<Session> session,shared_ptr<QueryStateMachine> stateMachine)
    {
        this->root = tree;
        this->rawTree = rawTree;
        this->session = session;
        this->stateMachine = stateMachine;
        this->executionFactory = make_shared<StageTreeExecutionFactory>(this->session);
        this->scheduleLock = make_shared<SqlScheduleLock>();


        this->stageExeSchedulers = this->executionFactory->createStageTreeExecutions(this->rootStage, this->root);

        for (auto stageExecution: stageExeSchedulers)
        {
            stageExecutionsMap[stageExecution.getStageExecution()->getStageId().getId()] = make_shared<StageExecutionAndScheduler>(stageExecution.getStageExecution(),
                                                                                                                       stageExecution.getStageLinkage(),stageExecution.getStageScheduler());
        }

        list<shared_ptr<SqlStageExecution>> stageExecutions;
        for(auto exe : this->stageExeSchedulers)
            stageExecutions.push_back(exe.getStageExecution());

        this->sidewayExchangeSystem = make_shared<SidewayExchangeSystem>(this->stageExeSchedulers,this->scheduleLock);

        this->stageProcessingTimeCollector = make_shared<StageProcessingTimeCollector>(stageExecutions);
        this->stageProcessingTimeCollector->start();
    }

    shared_ptr<SqlStageExecution> getRootStage()
    {
        return this->rootStage;
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

    set<int> getAllScalableStageIds()
    {
        set<int> ids;
        for(auto exe : this->stageExeSchedulers) {
            if(exe.getStageExecution()->isStageScalable() && !exe.getStageExecution()->getState()->isDone())
                ids.insert(exe.getStageExecution()->getStageId().getId());
        }
        return ids;
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

    void traversePlanToFindJoin(PlanNode *root, map<string,shared_ptr<SqlStageExecution>> &results)
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
            results[root->getId()] = findStageExecutionByTableScanId(tablescanId);
            //spdlog::info(root->getId()+"----"+ to_string(findStageExecutionByTableScanId(tablescanId)->getStageId().getId()));

        }

        auto childs = root->getSources();

        for(auto child : childs)
            traversePlanToFindJoin(child,results);

    }

    map<string,shared_ptr<SqlStageExecution>> getJoinIdToTableScanStage()
    {
        map<string,shared_ptr<SqlStageExecution>> result;
        auto planRoot = this->rawTree;
        traversePlanToFindJoin(planRoot,result);

        return result;
    }

    map<int,pair<long,long>>  getStageProcessingTimes()
    {
        auto joinIdToSqlStage = this->getJoinIdToTableScanStage();
        map<shared_ptr<SqlStageExecution>,long> stageToBuildTime;
        for(auto exe : this->stageExeSchedulers)
        {
            auto jbs = exe.getStageExecution()->getJoinIdToBuildTime();

            for(auto jb : jbs) {

                string jId;
                for(auto joinId : jb) {
                    if (joinIdToSqlStage.contains(joinId.first))
                    {
                        if(!stageToBuildTime.contains(joinIdToSqlStage[joinId.first])) {
                            stageToBuildTime[joinIdToSqlStage[joinId.first]] = 0;
                            jId = joinId.first;
                            stageToBuildTime[joinIdToSqlStage[joinId.first]] += joinId.second;
                        }
                        else {
                            if(joinId.second > stageToBuildTime[joinIdToSqlStage[joinId.first]])
                                stageToBuildTime[joinIdToSqlStage[joinId.first]] = joinId.second;
                        }
                    }
                }
                //stageToBuildTime[joinIdToSqlStage[jId]] /= jbs.size();
            }
        }

        map<int,pair<long,long>> results;
        auto processingTimes = this->stageProcessingTimeCollector->getStageProcessingTimes();
        for(auto pt : processingTimes) {
            auto buildTime = stageToBuildTime[pt.first];
            auto procTime = pt.second;
            pt.second+=buildTime;
            spdlog::info("Stage " + to_string(pt.first->getStageId().getId()) + ": " + to_string(pt.second)+"ms");
            if(pt.first->getFragment()->hasTableScan())
                results[pt.first->getStageId().getId()] = {procTime,buildTime};
        }

        return results;
    }

    long getCurProcessingTime(int stageId)
    {
        auto re = this->getStageProcessingTimes();
        return re[stageId].first;
    }

    map<int,int> getStageDOPs()
    {
        map<int,int> results;
        for(auto exe : this->stageExeSchedulers)
            results[exe.getStageExecution()->getStageId().getId()] = exe.getStageExecution()->getAllTasks().size();
        return results;
    }

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

    nlohmann::json getSidewayScheduleInfoJsons()
    {
        return this->sidewayExchangeSystem->getSchedulingTimes();
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


    void decreaseParallelismForOneStage(vector<StageExecutionAndScheduler> executions, int stageId,int degree)
    {
        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {
                shared_ptr<SqlStageExecution> stageExe = executions[i].getStageExecution();
                for(int de = 0 ; de < degree ; de++)
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
    void updateIntraTaskParallelismByTaskId(vector<StageExecutionAndScheduler> executions, int stageId,int taskId,shared_ptr<TaskIntraParaUpdateRequest> request)
    {
        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == (stageId)) {
                executions[i].getStageExecution()->updateTasksIntraParaByTaskId(taskId,request);
                this->setStageFirstExecutionTimePrediction(make_shared<StageExecutionAndScheduler>(executions[i].getStageExecution(),
                                                                                                   executions[i].getStageLinkage(),executions[i].getStageScheduler()));

            }
        }
    }

    void addGPUTaskForStage(int stageId) {
        auto executions = this->stageExeSchedulers;

        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {

                if (executions[i].getStageExecution()->isStageScalable()) {

                    shared_ptr<TaskExecutionCondition> condition = make_shared<TaskExecutionCondition>(TaskExecutionCondition::HETERO_TASK_SCHEDULE,"GPU");

                    auto result = (static_pointer_cast<NormalStageScheduler>(executions[i].getStageScheduler()))->addHeteroTask("GPU",condition);
                    if(result.getNewTasks().empty()) {
                        spdlog::info("No GPU node found!");
                       // return;
                    }
                    executions[i].getStageLinkage()->processScheduleResultsToAddConcurrent(result.getNewTasks());
                }
            }

        }
    }


    void closeGPUTaskForStage(int stageId) {
        auto executions = this->stageExeSchedulers;

        set<int> taskIdsWithExtension;
        shared_ptr<SqlStageExecution> stageExecution = NULL;
        for (int i = 0; i < executions.size(); i++) {
            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {
                if (executions[i].getStageExecution()->isStageScalable()) {
                    taskIdsWithExtension = executions[i].getStageExecution()->getTaskIdsWithExtension("GPU");
                    stageExecution = executions[i].getStageExecution();
                }
            }
        }

        if(stageExecution != NULL && !taskIdsWithExtension.empty())
        for(auto id : taskIdsWithExtension)
            stageExecution->finishTaskByTaskId(id);
    }


    //---------------------------------------------------Stage Info ----------------------------------------------------------//

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

    static void displayAllTasksThroughputsInfo(shared_ptr<SqlQueryScheduler> scheduler)
    {
        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;
        for (int i = 0; i < executions.size(); i++) {
            executions[i].getStageExecution()->displayEachTaskThroughputInfo();
        }
    }


    //---------------------------------------------------Dynamic schedule-----------------------------------------------------------//


    static void addStageConcurrent(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;


        if(!scheduler->scheduleLock->tune_lock())
            return;
        scheduler->addMulConcurrencyForOneStage(scheduler->stageExeSchedulers,stageId,1);
        scheduler->scheduleLock->tune_unlock();

    }

    static void addStageMulConcurrent(shared_ptr<SqlQueryScheduler> scheduler,int stageId,int taskNum)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;


        if(!scheduler->scheduleLock->tune_lock())
            return;
        scheduler->addMulConcurrencyForOneStage(scheduler->stageExeSchedulers,stageId,taskNum);
        scheduler->scheduleLock->tune_unlock();
    }

    static void addStageMulConcurrentByNodesGroup(shared_ptr<SqlQueryScheduler> scheduler,int stageId,int taskNum)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        if(!scheduler->scheduleLock->tune_lock())
            return;
        scheduler->addMulConcurrencyForOneStageByNodesGroup(scheduler->stageExeSchedulers,stageId,taskNum);
        scheduler->scheduleLock->tune_unlock();
    }

    static void decreaseStageParallelism(shared_ptr<SqlQueryScheduler> scheduler,int stageId,int degree)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        if(!scheduler->scheduleLock->tune_lock())
            return;
        scheduler->decreaseParallelismForOneStage(scheduler->stageExeSchedulers,stageId,degree);
        scheduler->scheduleLock->tune_unlock();
    }

    static void decreaseStageTaskGroupParallelism(shared_ptr<SqlQueryScheduler> scheduler,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        if(!scheduler->scheduleLock->tune_lock())
            return;
        scheduler->decreaseTaskGroupParallielismForOneStage(scheduler->stageExeSchedulers,stageId);
        scheduler->scheduleLock->tune_unlock();
    }


    static void addQueryConcurrency(shared_ptr<SqlQueryScheduler> scheduler,string degree)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;
        if(!scheduler->scheduleLock->tune_lock())
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

        scheduler->scheduleLock->tune_unlock();
    }

    static void addStageConcurrency(shared_ptr<SqlQueryScheduler> scheduler,string degree,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        if(!scheduler->scheduleLock->tune_lock())
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

        scheduler->scheduleLock->tune_unlock();

    }

    static void addQueryParallelism(shared_ptr<SqlQueryScheduler> scheduler,string degree)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;
        if(!scheduler->scheduleLock->tune_lock())
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

        scheduler->scheduleLock->tune_unlock();
    }

    static void addQueryParallelismUsingInitialNodes(shared_ptr<SqlQueryScheduler> scheduler,string degree)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        if(!scheduler->scheduleLock->tune_lock())
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

        scheduler->scheduleLock->tune_unlock();
    }


    static void addStageAllTaskIntraPipelineConcurrent(shared_ptr<SqlQueryScheduler> scheduler,string stageId,string pipelineId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;
        if(!scheduler->scheduleLock->tune_lock())
            return;


        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>(pipelineId,"incre","1");
        scheduler->updateIntraTaskParallelism(scheduler->stageExeSchedulers,atoi(stageId.c_str()),intraRequest);

        scheduler->scheduleLock->tune_unlock();
    }


    static void addStageTaskIntraExtensionPipelineConcurrentByTaskId(shared_ptr<SqlQueryScheduler> scheduler,string extensionType,string stageId,int taskId,string pipelineId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;
        if(!scheduler->scheduleLock->tune_lock())
            return;


        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>(pipelineId,"incre","1",extensionType);
        scheduler->updateIntraTaskParallelismByTaskId(scheduler->stageExeSchedulers,atoi(stageId.c_str()),taskId,intraRequest);

        scheduler->scheduleLock->tune_unlock();

    }

    static void closeStageTaskIntraExtensionPipelineConcurrentByTaskId(shared_ptr<SqlQueryScheduler> scheduler,string extensionType,string stageId,int taskId,string pipelineId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;
        if(!scheduler->scheduleLock->tune_lock())
            return;

        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>(pipelineId,"decre","1",extensionType);
        scheduler->updateIntraTaskParallelismByTaskId(scheduler->stageExeSchedulers,atoi(stageId.c_str()),taskId,intraRequest);

        scheduler->scheduleLock->tune_unlock();

    }

    static void closeStageAllTaskIntraPipelineConcurrent(shared_ptr<SqlQueryScheduler> scheduler,string stageId,string pipelineId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;
        if(!scheduler->scheduleLock->tune_lock())
            return;

        shared_ptr<TaskIntraParaUpdateRequest> intraRequest  = make_shared<TaskIntraParaUpdateRequest>(pipelineId,"decre","1");
        scheduler->updateIntraTaskParallelism(scheduler->stageExeSchedulers,atoi(stageId.c_str()),intraRequest);

        scheduler->scheduleLock->tune_unlock();
    }

   //-------------------------------------------------Sideway Missions-----------------------------------------------------------------//


    static bool moveTaskOperatorTest(shared_ptr<SqlQueryScheduler> scheduler,string para)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return false;


        vector<StageExecutionAndScheduler> executions = scheduler->stageExeSchedulers;

        if(para == "move") {

            scheduler->sidewayExchangeSystem->submitSidewayExchangeTask(0,{0},SidewayDataExchangeScheduler::OPERATOR_MIGRATION);
            return true;
        }
        else if(para == "moveh")
        {
            scheduler->sidewayExchangeSystem->submitSidewayExchangeTask(1,{0},SidewayDataExchangeScheduler::OPERATOR_MIGRATION);
            return true;
        }
        else if(para == "clone")
        {
            scheduler->sidewayExchangeSystem->submitSidewayExchangeTask(1,{0,1},SidewayDataExchangeScheduler::OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN);
            return true;
        }
        else if(para == "buffer")
        {
            scheduler->sidewayExchangeSystem->submitSidewayExchangeTask(3,{0},SidewayDataExchangeScheduler::BUFFER_MIGRATION,SidewayDataExchangeScheduler::MANY_TO_MANY,3);
            return true;
        }

        return false;
    }

    static bool submitTaskMigrationMission(shared_ptr<SqlQueryScheduler> scheduler,int stageId,vector<int> taskIds)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return false;

        scheduler->sidewayExchangeSystem->submitTaskMigrationMission(stageId,taskIds);
        return true;
    }

    static bool submitBufferMigrationMission(shared_ptr<SqlQueryScheduler> scheduler,int stageId,vector<int> taskIds,int targetTaskNums)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return false;

        if(targetTaskNums <= 0)
            scheduler->sidewayExchangeSystem->submitBufferMigrationMission(stageId,taskIds,SidewayDataExchangeScheduler::ONE_TO_ONE,-1);
        else
            scheduler->sidewayExchangeSystem->submitBufferMigrationMission(stageId,taskIds,SidewayDataExchangeScheduler::MANY_TO_MANY,targetTaskNums);

        return true;
    }

    static void moveFinishedTaskDataToNewNodeForStage(shared_ptr<SqlQueryScheduler> scheduler,int stageId,int taskId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        auto executions = scheduler->stageExeSchedulers;

        for (int i = 0; i < executions.size(); i++) {

            if (executions[i].getStageExecution()->getStageId().getId() == stageId) {

                SidewayDataExchangeScheduler scheduler(SidewayDataExchangeScheduler::BUFFER_MIGRATION,
                                                       executions[i].getStageExecution(),
                                                       executions[i].getStageScheduler(),
                                                       executions[i].getStageLinkage(),
                                                       {taskId});
                scheduler.schedule();
            }

        }
    }

    static void drainNode(shared_ptr<SqlQueryScheduler> scheduler,string nodeUrl)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;
        scheduler->sidewayExchangeSystem->processNodeDraining(nodeUrl);

    }

    static void addHeteroTaskForStage(shared_ptr<SqlQueryScheduler> scheduler,string nodeType,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        if(nodeType != "GPU") {
            spdlog::warn("Unsupported heterogeneous type "+nodeType+"!");
            return;
        }
        auto executions = scheduler->stageExeSchedulers;

        for (int i = 0; i < executions.size(); i++) {
            if (executions[i].getStageExecution()->getStageId().getId() == stageId)
               scheduler->addGPUTaskForStage(stageId);
        }
    }

    static void closeHeteroTaskForStage(shared_ptr<SqlQueryScheduler> scheduler,string nodeType,int stageId)
    {
        if(scheduler->stateMachine->isFinished() || !scheduler->canIQRS())
            return;

        if(nodeType != "GPU") {
            spdlog::warn("Unsupported heterogeneous type "+nodeType+"!");
            return;
        }
        auto executions = scheduler->stageExeSchedulers;
        scheduler->closeGPUTaskForStage(stageId);
    }




    //-------------------------------------------------------static schedule--------------------------------------------------------------------------------//

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


        shared_ptr<DataPage> result;
        scheduler->openIQRS();
        spdlog::debug(scheduler->session->getQueryId() +" initial schedule finished!");

        Timer timer;
        bool infoTag = true;


        for(;;)
        {

            int counter = 0;

            for(auto exe : executions)
            {
                if(exe.getStageExecution()->getState()->isDone())
                    counter++;
                else
                    exe.getStageExecution()->getStateChangeListener()->listen();
            }

            if(counter == executions.size())
            {
                if(!scheduler->sidewayExchangeSystem->noSidewayTasks())
                    scheduler->sidewayExchangeSystem->listenAllMissionsStatus();
                else {
                    if(infoTag == true) {
                        spdlog::info("Query finished!");
                        infoTag = false;
                    }
                    scheduler->stateMachine->finished();
                }
            }


            if(scheduler->stateMachine->isFinished()) {

                    shared_ptr<TaskResultFetcher> taskResultFetcher = NULL;

                    taskResultFetcher = scheduler->rootStage->getTaskResultFetcher();
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

        }

        scheduler->originTime = originTime;
        scheduler->actualTime = actualTime;
        scheduler->executionTime = exeTime;

           vector<string> results = scheduler->getStagesInfo();
        for(auto re : results)
        {
            spdlog::info(re);
        }

        scheduler->getStageProcessingTimes();


        if(originTime > actualTime)
            spdlog::info("Origin:"+to_string(originTime)+" Actual:"+to_string(actualTime)+" Saved "+ to_string((originTime-actualTime)/originTime) +"% percent of time!");
        else
            spdlog::info("Execution time:"+to_string(scheduler->getExecutionTime()));

    }



};


#endif //OLVP_SQLQUERYSCHEDULER_HPP
