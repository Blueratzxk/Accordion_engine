//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SQLQUERYEXECUTION_HPP
#define OLVP_SQLQUERYEXECUTION_HPP


#include "../../Frontend/PlanNode/PlanNode.hpp"
#include "../../Frontend/PlanNode/PlanNodeTreeToJson.hpp"
#include "SqlQueryScheduler.hpp"
#include "../../Planner/PlanTreeAnalyzer.hpp"
#include "../../Query/QueryStateMachine.hpp"
#include "../Tuning/ProgressAndTuner.hpp"

class Dynamic_scheduler
{
    shared_ptr<SqlQueryScheduler> scheduler;


public:
    Dynamic_scheduler(shared_ptr<SqlQueryScheduler> scheduler)
    {
        this->scheduler = scheduler;
    }

    string addStageConcurrent(int stageId)
    {
        thread(SqlQueryScheduler::addStageConcurrent,this->scheduler,stageId).detach();
        return "OK";
    }

    string decreaseStageParallelism(int stageId)
    {
        thread(SqlQueryScheduler::decreaseStageParallelism,this->scheduler,stageId).detach();
        return "OK";
    }

    string decreaseStageTaskGroupParallelism(int stageId,int taskNum)
    {
        thread(SqlQueryScheduler::decreaseStageTaskGroupParallelism,this->scheduler,stageId).detach();
        return "OK";
    }

    string addStageTaskGroupConcurrent(int stageId,int taskNum)
    {
        thread(SqlQueryScheduler::addStageMulConcurrent,this->scheduler,stageId,taskNum).detach();
        return "OK";
    }

    string addStageTaskGroupConcurrentToInit(int stageId,int taskNum)
    {
        thread(SqlQueryScheduler::addStageMulConcurrentByNodesGroup,this->scheduler,stageId,taskNum).detach();
        return "OK";
    }


    string addQueryParallelism(int degree)
    {
        thread(SqlQueryScheduler::addQueryParallelism,this->scheduler, to_string(degree)).detach();
        return "OK";
    }
    string addQueryParallelismUsingInitialNodes(int degree)
    {
        thread(SqlQueryScheduler::addQueryParallelismUsingInitialNodes,this->scheduler, to_string(degree)).detach();
        return "OK";
    }


    string addQueryConcurrency(int degree)
    {
        thread(SqlQueryScheduler::addQueryConcurrency,this->scheduler, to_string(degree)).detach();
        return "OK";
    }

    string addStageConcurrency(int stageId,int degree)
    {
        thread(SqlQueryScheduler::addStageConcurrency,this->scheduler, to_string(degree),stageId).detach();
        return "OK";
    }



    string addStageAllTaskIntraPipelineConcurrent(string stageId,string pipelineId)
    {

        thread(SqlQueryScheduler::addStageAllTaskIntraPipelineConcurrent,this->scheduler,stageId,pipelineId).detach();
        return "OK";
    }
    string subStageAllTaskIntraPipelineConcurrent(string stageId,string pipelineId)
    {

        thread(SqlQueryScheduler::closeStageAllTaskIntraPipelineConcurrent,this->scheduler,stageId,pipelineId).detach();
        return "OK";
    }
};


class DynamicTuningMonitor
{
    shared_ptr<SqlQueryScheduler> scheduler;

public:
    DynamicTuningMonitor(shared_ptr<SqlQueryScheduler> scheduler)
    {
        this->scheduler = scheduler;
    }

    bool isBuildTimeTooLongForStage(int stageId)
    {
        auto stageObj = this->scheduler->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution();

        double buildTime;

        if(stageObj->isBufferNeedPartitioning())
            buildTime = stageObj->getMaxHashTableBuildTimeofTasks();
        else
            buildTime = stageObj->getMaxHashTableBuildComputingTimeofTasks();

        if(buildTime <= 0)
            return false;

        auto tableScanStage = this->scheduler->findRootTableScanStageForStage(to_string(stageId));

      //  double remainingTuple = tableScanStage->getStageExecution()->getRemainingTupleCount();
  //      double avgThroughput = tableScanStage->getStageExecution()->getRemainingTupleRate();


        double remainingTime = tableScanStage->getStageExecution()->getRemainingTime();
     //   if(remainingTuple == 0 || avgThroughput == 0)
    //        remainingTime = 0;
   //     else
    //        remainingTime = remainingTuple/avgThroughput;

        if(remainingTime < buildTime) {
            spdlog::info("Monitor find increasing this DOP now is not a good idea!");
            return true;
        }
        return false;


    }


};

class SqlQueryExecution
{
    PlanNode *PlanNodeRoot;
    shared_ptr<SqlQueryScheduler> scheduler;
    shared_ptr<Session> session;
    shared_ptr<QueryStateMachine> stateMachine;
    shared_ptr<Dynamic_scheduler> dyScheduler;
    shared_ptr<DynamicTuningMonitor> dyMonitor;


    bool isStageTuningKnob(shared_ptr<SqlStageExecution> stage)
    {
        if(stage->getFragment()->hasNodeType("FinalAggregationNode") ||
        stage->getFragment()->hasNodeType("TaskOutputNode")||
        stage->getFragment()->hasNodeType("TopKNode")||
        stage->getFragment()->hasNodeType("TableScanNode"))
            return false;
        return true;
    }


    void extractProgressAndTuner(shared_ptr<SqlStageExecution> stage,shared_ptr<ProgressAndTuner> progressAndTuner)
    {
        shared_ptr<SqlStageExecution> tempStage = stage;
        list<shared_ptr<ProgressAndTuner>> dependencies;

        if(tempStage->getFragment()->hasTableScan())
        {
            progressAndTuner->setTableScanProgress(tempStage->getStageId().getId());
            progressAndTuner->setExecutionDependencies(dependencies);
            return;
        }

        do {
            if(this->isStageTuningKnob(tempStage))
                progressAndTuner->addTuningKnobStage(tempStage->getStageId().getId());

            shared_ptr<StageExecutionAndScheduler> seas = this->scheduler->getStageExecutionAndSchedulerByStagId(
                    tempStage->getStageId().getId());
            auto childStages = seas->getStageLinkage()->getChildStages();
            if(!childStages.empty())
                tempStage = childStages[0];

            if(childStages.size() > 1)
            {

                for(int i = 1 ; i < childStages.size() ; i++) {
                    auto newProgressAndTuner = make_shared<ProgressAndTuner>();
                    extractProgressAndTuner(childStages[i], newProgressAndTuner);
                    dependencies.push_back(newProgressAndTuner);
                }
            }
        }
        while(!tempStage->getFragment()->hasTableScan());
        progressAndTuner->setTableScanProgress(tempStage->getStageId().getId());
        progressAndTuner->setExecutionDependencies(dependencies);
    }


public:
    SqlQueryExecution(shared_ptr<Session> session,PlanNode *root){

        this->PlanNodeRoot = root;
        this->session = session;
        this->stateMachine = make_shared<QueryStateMachine>();

    }

    shared_ptr<map<int,shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>>>> getStagesNodeTaskMap()
    {

        return this->scheduler->getStagesNodeTaskMap();
    }
    map<int,list<taskCpuNetUsageInfo>> getStagesCpuUsages()
    {

        return this->scheduler->getStagesCpuUsages();
    }

    map<int,long> getStageProcessingTimes()
    {
        return this->scheduler->getStageProcessingTimes();
    }

    map<int,int> getStageDOPs()
    {
        return this->scheduler->getStageDOPs();
    }

    double getSingleTaskCpuUsageOfStage(int stageId)
    {
        return this->scheduler->getSingleTaskOfStageCpuUsage(stageId);
    }

    double getRemainingTaskCpuUsageOfStageByTaskThreadNums(int stageId)
    {
        double result = this->scheduler->getStageRemainingCpuUsageRatioByTaskThreadNums(stageId);

        return result;
    }

    void planDistribution(PlanNode *root)
    {
        stateMachine->planned();
        PlanTreeAnalyzer analyzer(root);
        shared_ptr<SubPlan> tree = analyzer.analyzeToSubPlanTree();
        this->scheduler = make_shared<SqlQueryScheduler>(root,tree,this->session,this->stateMachine);
        this->dyScheduler = make_shared<Dynamic_scheduler>(this->scheduler);
        this->dyMonitor = make_shared<DynamicTuningMonitor>(this->scheduler);
    }

    shared_ptr<QueryStateMachine> getState()
    {
        return this->stateMachine;
    }

    shared_ptr<Session> getSession()
    {
        return this->session;
    }

    shared_ptr<SqlQueryScheduler> getScheduler()
    {
        return this->scheduler;
    }



    shared_ptr<ProgressAndTuner> getProgressAndTuner()
    {
        shared_ptr<ProgressAndTuner> result = make_shared<ProgressAndTuner>();
        this->extractProgressAndTuner(this->scheduler->getRootStage(),result);
        return result;
    }

    bool isQueryFinished()
    {
        return this->stateMachine->isFinished();
    }
    bool isQueryCanceled()
    {
        return this->stateMachine->isCanceled();
    }

    bool isQueryStart()
    {
        return this->stateMachine->isRunning();
    }

    void start()
    {
        planDistribution(this->PlanNodeRoot);
        thread(this->scheduler->schedule,scheduler).detach();
    }

    string getQueryExecutionInfo()
    {

        if(this->stateMachine->getState() == QueryStateMachine::PLANNED)
        {
            return "[]";
        }
        if(this->stateMachine->getState() == QueryStateMachine::CANCELED)
        {
            return "[]";
        }
        vector<string> infos = this->scheduler->getStagesInfo();

        string result;
        result.append("[");

        for(auto info : infos)
        {
            result.append(info);
            result.append(",");
        }
        if(!infos.empty())
            result.pop_back();

        result.append("]");


        nlohmann::json json = nlohmann::json::parse(result);

        string jsonResult = json.dump(2);

        return jsonResult;
    }

    nlohmann::json getQueryExecutionInfoObj()
    {

        if(this->stateMachine->getState() == QueryStateMachine::PLANNED)
        {
            return "[]";
        }

        vector<string> infos = this->scheduler->getStagesInfo();

        string result;
        result.append("[");

        for(auto info : infos)
        {
            result.append(info);
            result.append(",");
        }
        if(!infos.empty())
            result.pop_back();

        result.append("]");


        nlohmann::json json = nlohmann::json::parse(result);


        return json;
    }

    string getQueryExecutionInfoString()
    {

        if(this->stateMachine->getState() == QueryStateMachine::PLANNED)
        {
            return "[]";
        }

        vector<string> infos = this->scheduler->getStagesInfo();

        string result;
        result.append("[");

        for(auto info : infos)
        {
            result.append(info);
            result.append(",");
        }
        if(!infos.empty())
            result.pop_back();

        result.append("]");


        return result;

    }

    string planTreeToJson()
    {
        PlanNodeTreeToJson pj;
        return pj.getJson(this->PlanNodeRoot);

    }
    nlohmann::json planTreeToJsonObj()
    {
        PlanNodeTreeToJson pj;
        return pj.getJsonObj(this->PlanNodeRoot);

    }

    string getQueryResult()
    {

        if(!this->stateMachine->isFinished())
        {
            return "{\"status\":\"Task_Unfinished\"}";
        }




        if(this->scheduler->getResultSet().empty())
            return "{\"status\":\"Result_Empty\"}";

        string resultBatch = ArrowRecordBatchViewer::BatchRowsToString(this->scheduler->getResultSet().front()->get());
        //this->scheduler->getResultSet().pop_front();

        return resultBatch;

    }

    map<int,map<int,string>> getQueryBuildRecords()
    {
        return this->scheduler->getStagesBuildRecords();
    }

    string getQueryExecutionTime()
    {
        return this->scheduler->getRootStageExecutionTime();
    }

    string getQueryThroughputsSnapShot()
    {

        return this->scheduler->getQueryStagesThroughputs(this->scheduler);
    }

    string getQueryThroughputsInfoSnapShot()
    {

        return this->scheduler->getQueryStagesThroughputsInfo(this->scheduler);
    }

    bool isStageScalable(int stageId)
    {
        if(this->scheduler == NULL)
            return false;

        return this->scheduler->isStageScalable(this->scheduler,stageId);
    }
    bool isStageExist(int stageId)
    {
        return this->scheduler->isStageExist(this->scheduler,stageId);
    }


    string Dynamic_addStageConcurrent(int stageId)
    {
        if(! this->dyMonitor->isBuildTimeTooLongForStage(stageId)) {
            this->dyScheduler->addStageConcurrent(stageId);
            return "YES";
        }
        else
            return "NO";
    }

    string Dynamic_decreaseStageParallelism(int stageId)
    {
        this->dyScheduler->decreaseStageParallelism(stageId);
        return "OK";
    }

    string Dynamic_decreaseStageTaskGroupParallelism(int stageId,int taskNum)
    {
        this->dyScheduler->decreaseStageTaskGroupParallelism(stageId,taskNum);

        return "OK";
    }

    string Dynamic_addStageTaskGroupConcurrent(int stageId,int taskNum)
    {
        if(! this->dyMonitor->isBuildTimeTooLongForStage(stageId)) {
            this->dyScheduler->addStageTaskGroupConcurrent(stageId, taskNum);
            return "YES";
        }
        else
            return "NO";
    }

    string Dynamic_addStageTaskGroupConcurrentToInit(int stageId,int taskNum)
    {
        this->dyScheduler->addStageTaskGroupConcurrentToInit(stageId,taskNum);

        return "OK";
    }

    string Dynamic_addQueryParallelism(int degree)
    {
        this->dyScheduler->addQueryParallelism(degree);
        return "OK";
    }
    string Dynamic_addQueryParallelismUsingInitialNodes(int degree)
    {
        this->dyScheduler->addQueryParallelismUsingInitialNodes(degree);
        return "OK";
    }


    string Dynamic_addQueryConcurrency(int degree)
    {
        this->dyScheduler->addQueryConcurrency(degree);
        return "OK";
    }

    string Dynamic_addStageConcurrency(int stageId,int degree)
    {
        this->dyScheduler->addStageConcurrency(stageId,degree);
        return "OK";
    }
    string Dynamic_addStageAllTaskIntraPipelineConcurrent(string stageId,string pipelineId)
    {

        this->dyScheduler->addStageAllTaskIntraPipelineConcurrent(stageId,pipelineId);
        return "OK";
    }
    string Dynamic_subStageAllTaskIntraPipelineConcurrent(string stageId,string pipelineId)
    {

        this->dyScheduler->subStageAllTaskIntraPipelineConcurrent(stageId,pipelineId);
        return "OK";
    }




};


#endif //OLVP_SQLQUERYEXECUTION_HPP
