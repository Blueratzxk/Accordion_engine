//
// Created by zxk on 7/9/24.
//

#ifndef OLVP_PPM_HPP
#define OLVP_PPM_HPP

//Performance prediction module

#include "../NodeCluster/NodesManager.h"
#include "../Execution/Scheduler/SqlQueryExecution.hpp"
#include "ClusterContext.hpp"

class smallWindow
{
    int index = 0;
    vector<double> values;
    int windowLength = 20;
public:

    smallWindow()
    {

    }
    smallWindow(int windowLength)
    {
        this->windowLength = windowLength;
    }
    double getAvg()
    {
        double all = 0.0;
        for(auto val : values)
            all+=val;
        if(values.size() < windowLength)
        {
            return all/values.size();
        }
        else
            return all/windowLength;
    }
    void addValue(double value)
    {
        if(index%windowLength >= values.size())
        {
            values.push_back(value);
            index++;
            index = index%(windowLength);
        }
        else
        {
            (values)[index] = value;
            index++;
            index = index%(windowLength);
        }
    }
};
class QueryRecorder
{
public:
    int counter = 0;
    int windowLength = 20;
    map<int,map<shared_ptr<ClusterNode>,shared_ptr<smallWindow>>> record;


    QueryRecorder(){}


    double getStageMinImprovement(int stageId)
    {
        double min = 99999999;
        if(record.count(stageId) > 0)
        {
            for(auto im : record[stageId])
            {
                if(im.second->getAvg() < min)
                    min = im.second->getAvg();
            }
        }
        return min;
    }

    double getStageAVGImprovement(int stageId)
    {

        double total = 0;
        int num = 0;
        if(record.count(stageId) > 0)
        {
            for(auto im : record[stageId])
            {
                total += im.second->getAvg();
                num++;
            }
        }

        return total/(double)num;
    }

    void view()
    {

        for(auto stage : record) {
            spdlog::info("For stage : " + to_string(stage.first));
            for (auto node: stage.second) {

                spdlog::info("On node " + node.first->getNodeLocation() + " can improve " + to_string(node.second->getAvg()));
            }
        }

    }


};


class CpuBottleneckResult
{
    int stageId;
    string node;
    set<int> taskIds;
    double maxValue;
    double curValue;
public:
    CpuBottleneckResult(int stageId, string node, set<int> taskIds,double maxValue,double curValue){
        this->stageId = stageId;
        this->node = node;
        this->taskIds = taskIds;
        this->maxValue = maxValue;
        this->curValue = curValue;
    }

    int getStageId(){return this->stageId;}
    double getMaxValue(){return this->maxValue;}
    double getCurValue(){return this->curValue;}
    string getNode(){return this->node;};
    set<int> getTaskIds(){return this->taskIds;};

};
class NetBottleneckResult
{
    int stageId;
    string node;
    set<int> taskIds;
    double maxValue;
    double curValue;
public:
    NetBottleneckResult(int stageId, string node, set<int> taskIds,double maxValue,double curValue)
    {
        this->stageId = stageId;
        this->node = node;
        this->taskIds = taskIds;
        this->maxValue = maxValue;
        this->curValue = curValue;
    }

    int getStageId(){return this->stageId;}
    double getMaxValue(){return this->maxValue;}
    double getCurValue(){return this->curValue;}
    string getNode(){return this->node;};
    set<int> getTaskIds(){return this->taskIds;};

};

class QueryBottleneckResult
{
public:
    enum type{net_rec,net_out,compute};
private:
    int stageId;
    type bottleneckType;
public:

    QueryBottleneckResult(int stageId,type bottleneckType)
    {
        this->stageId = stageId;
        this->bottleneckType = bottleneckType;
    }
    int getStageId(){return this->stageId;}
    string getBottleneckType(){
        if(this->bottleneckType == net_rec )
            return "net receive";
        else if(this->bottleneckType == net_out)
            return "net output";
        else
            return "compute";}
};

class StageTaskMaxImproveRatioResult
{
    int stageId;
    double maxNodeCpuRatio;
    double maxThreadCpuRatio;

public:
    StageTaskMaxImproveRatioResult(int stageId,double maxNodeRatio,double maxThreadRatio)
    {
        this->stageId = stageId;
        this->maxNodeCpuRatio = maxNodeRatio;
        this->maxThreadCpuRatio = maxThreadRatio;
    }

    int getStageId()
    {
        return this->stageId;
    }
    double getMaxRatio()
    {
        return this->maxNodeCpuRatio;
    }
    double getMaxRatioOfThreads()
    {
        return this->maxThreadCpuRatio;
    }
};

class PPM
{
    shared_ptr<NodesManager> nodesManager;
    shared_ptr<ClusterContext> clusterContext;


    shared_ptr<SqlQueryExecution> queryToPredict;


    map<shared_ptr<ClusterNode>,float> nodeCpuUsageSnap;
    map<int,shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>>> stagesNodeTaskMapSnap;

    shared_ptr<map<string,shared_ptr<SqlQueryExecution>>> queries;





    //-----------------------------------results---------------------------------------------------//
    map<string,shared_ptr<QueryRecorder>> query_stage_node_net_improvement;
    map<string,shared_ptr<QueryRecorder>> query_stage_node_shuffle_improvement;
    map<string,pair<shared_ptr<QueryRecorder>,shared_ptr<QueryRecorder>>> query_stage_node_cpu_improvement;
    map<string,vector<shared_ptr<CpuBottleneckResult>>> cpuBottleneckResult;
    map<string,vector<shared_ptr<NetBottleneckResult>>> netTransBottleneckResult;
    map<string,vector<shared_ptr<NetBottleneckResult>>> netRecBottleneckResult;
    map<string,vector<shared_ptr<QueryBottleneckResult>>> queryBottleneckResult;
    map<string,vector<shared_ptr<StageTaskMaxImproveRatioResult>>> stageTaskMaxImproveRatioResult;



    bool refresh = false;
    bool stop = false;
    atomic<bool> start = false;
public:
    PPM(shared_ptr<map<string,shared_ptr<SqlQueryExecution>>> queries,shared_ptr<NodesManager> nodesManager)
    {
        this->nodesManager = nodesManager;
        this->queries = queries;
        this->clusterContext = make_shared<ClusterContext>(this->queries);
        thread(reCollect,this).detach();
    }
    bool isStart()
    {
        return this->start;
    }

    static void reCollect(PPM *ppm)
    {
        ppm->start = true;
        while(1)
        {
            ppm->collect();

            if(ppm->stop)
                return;
            sleep_for(std::chrono::milliseconds(800));
        }
    }

    void show()
    {
        this->showAllAnalyze();
    }

    void collect()
    {
        this->clusterContext->gatherInfos();
        for(auto query : (*this->queries)) {
            if (query.second->isQueryStart() && !query.second->isQueryFinished()) {
                this->queryToPredict = query.second;
                this->updateCpuUsageSnap();

                this->cpuBottleneckResult[query.first] = this->analyzeCpuBottleneckForStages();
                this->netTransBottleneckResult[query.first] = this->analyzeTransNetBottlenecksForStages();
                this->netRecBottleneckResult[query.first] = this->analyzeRecNetBottlenecksForStages();


                if(this->query_stage_node_net_improvement.count(query.first) > 0) {
                    auto rec = query_stage_node_net_improvement[query.first];
                    if( rec!= NULL)
                        this->analyzeMaxNetThroughputImprovementForStages(rec);
                }
                else {
                    shared_ptr<QueryRecorder> rec = make_shared<QueryRecorder>();
                    query_stage_node_net_improvement[query.first] = rec;

                    this->analyzeMaxNetThroughputImprovementForStages(rec);
                }


                if(this->query_stage_node_cpu_improvement.count(query.first) > 0) {
                    auto rec = query_stage_node_cpu_improvement[query.first];
                    if( rec.first!= NULL && rec.second!=NULL)
                        this->analyzeMaxCpuRateImprovementForStages(rec);

                }
                else {
                    shared_ptr<QueryRecorder> recByNode = make_shared<QueryRecorder>();
                    shared_ptr<QueryRecorder> recByThreads = make_shared<QueryRecorder>();
                    auto recordPair = make_pair(recByNode,recByThreads);
                    query_stage_node_cpu_improvement[query.first] = recordPair;
                    this->analyzeMaxCpuRateImprovementForStages(recordPair);
                }

                if(this->query_stage_node_shuffle_improvement.count(query.first) > 0) {
                    auto rec = query_stage_node_shuffle_improvement[query.first];
                    if( rec!= NULL)
                        this->analyzeMaxShuffleRateImprovementForStages(rec);

                }
                else {
                    shared_ptr<QueryRecorder> rec = make_shared<QueryRecorder>();
                    query_stage_node_shuffle_improvement[query.first] = rec;
                    this->analyzeMaxShuffleRateImprovementForStages(rec);
                }

                stageTaskMaxImproveRatioResult[query.first] = analyzeMaxStageCpuUsageImprovementOnCurrentCluster();
                queryBottleneckResult[query.first] = analyzeQueryBottleneck();
            }
            else
            {
                query_stage_node_net_improvement.erase(query.first);
                query_stage_node_shuffle_improvement.erase(query.first);
                query_stage_node_cpu_improvement.erase(query.first);
                cpuBottleneckResult.erase(query.first);
                netTransBottleneckResult.erase(query.first);
                netRecBottleneckResult.erase(query.first);
                queryBottleneckResult.erase(query.first);
                stageTaskMaxImproveRatioResult.erase(query.first);
            }
        }
    }


    void test() {
        for (auto query: (*this->queries)) {
            if (query.second->isQueryStart() && !query.second->isQueryFinished()) {

                auto getstages = query.second->getStagesNodeTaskMap();

                for(auto stage : (*getstages))
                {
                    spdlog::info(query.first+" StageId:" + to_string(stage.first));
                    this->testStageImprovementPredictionExtern(query.first,stage.first,5);
                }
                spdlog::info("+++++++++++++++++++++++++++++++++++++++++++++++++++++");

                for(auto stage : (*getstages))
                {
                    spdlog::info(query.first+" StageId:" + to_string(stage.first));
                    this->testStageImprovementPredictionExtern(query.first,stage.first,20);
                }
                spdlog::info("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
                for(auto stage : (*getstages))
                {
                    spdlog::info(query.first+" StageId:" + to_string(stage.first));
                    this->testStageImprovementPredictionExtern(query.first,stage.first,200);
                }

            }
        }
    }


    void updateCpuUsageSnap()
    {
        auto allNodes = nodesManager->getAllNodes();
        for(auto node : allNodes)
            nodeCpuUsageSnap[node] = node->getCurrentCpuUsage();

        if(this->queryToPredict->getStagesNodeTaskMap() != NULL)
            this->stagesNodeTaskMapSnap = *(this->queryToPredict->getStagesNodeTaskMap());


    }
    vector<shared_ptr<CpuBottleneckResult>> analyzeCpuBottleneckForStages()
    {
        vector<shared_ptr<CpuBottleneckResult>> results;
        for(auto stage : this->stagesNodeTaskMapSnap)
        {
            auto re = this->analyzeCpuBottleneckForStage(stage.first);

            for(auto res : re)
            {
                results.push_back(res);
            }
        }
        return results;
    }

    vector<shared_ptr<CpuBottleneckResult>> analyzeCpuBottleneckForStage(int stageId)
    {

        vector<shared_ptr<CpuBottleneckResult>> results;

        map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>> stageTasks = (*this->stagesNodeTaskMapSnap[stageId]);

        set<int> tasks;

        for(auto node : stageTasks)
        {

            float maxCpuUsage = node.first->getCoreNums()*100.0;
            float currentCpuUsage = node.first->getCurrentCpuUsage();


            for(auto task: node.second) {
                tasks.insert(task->getTaskId()->getId());
            }

            if(maxCpuUsage-currentCpuUsage < 100 && !tasks.empty())
            {
                shared_ptr<CpuBottleneckResult> result = make_shared<CpuBottleneckResult>(stageId,node.first->getNodeLocation(),tasks,maxCpuUsage,currentCpuUsage);
                results.push_back(result);
            }

        }


        return results;
    }

    vector<shared_ptr<NetBottleneckResult>> analyzeTransNetBottlenecksForStages()
    {
        vector<shared_ptr<NetBottleneckResult>> transResult;
        for(auto stage : this->stagesNodeTaskMapSnap)
        {
            auto re = this->isEncounterNetTransBottleneckForStage(stage.first);

            for(auto res : re)
                transResult.push_back(res);
        }
        return transResult;
    }

    vector<shared_ptr<NetBottleneckResult>> analyzeRecNetBottlenecksForStages()
    {

        vector<shared_ptr<NetBottleneckResult>> recResult;
        for(auto stage : this->stagesNodeTaskMapSnap)
        {
            auto re = this->isEncounterNetRecBottleneckForStage(stage.first);
            for(auto res : re)
                recResult.push_back(res);
        }

        return recResult;
    }

    vector<shared_ptr<NetBottleneckResult>> isEncounterNetTransBottleneckForStage(int stageId)
    {
        map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>> stageTasks = (*this->stagesNodeTaskMapSnap[stageId]);

        string taskIdStr;
        set<int> tasks;
        bool hasBottleneck = false;

        vector<shared_ptr<NetBottleneckResult>> results;

        for(auto node : stageTasks)
        {
            for(auto task: node.second) {
                tasks.insert(task->getTaskId()->getId());
            }

            float maxSpeed = ((float)node.first->getNetSpeed())*1.024/8*1000;
            if(node.first->hitTransBottleNeck() && !tasks.empty())
            {
                shared_ptr<NetBottleneckResult> result = make_shared<NetBottleneckResult>(stageId,node.first->getNodeLocation(),tasks,maxSpeed,node.first->getCurrentNetTransRate());
                results.push_back(result);
            }
        }

        return results;

    }
    vector<shared_ptr<NetBottleneckResult>> isEncounterNetRecBottleneckForStage(int stageId)
    {
        map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>> stageTasks = (*this->stagesNodeTaskMapSnap[stageId]);

        vector<shared_ptr<NetBottleneckResult>> results;
        set<int> tasks;
        for(auto node : stageTasks)
        {
            for(auto task: node.second) {
                tasks.insert(task->getTaskId()->getId());
            }

            float maxSpeed = ((float)node.first->getNetSpeed())*1.024/8*1000;
            if(node.first->hitRecBottleNeck() && !tasks.empty())
            {
                shared_ptr<NetBottleneckResult> result = make_shared<NetBottleneckResult>(stageId,node.first->getNodeLocation(),tasks,maxSpeed,node.first->getCurrentNetRecRate());
                results.push_back(result);
            }
        }


        return results;

    }
    void analyzeMaxNetThroughputImprovementForStages(shared_ptr<QueryRecorder> preImprovement)
    {
        map<int,map<shared_ptr<ClusterNode>,vector<double>>> newImprovements;

        for(auto stage : this->stagesNodeTaskMapSnap)
        {

            if(preImprovement->record.count(stage.first) > 0)
                this->analyzeMaxNetThroughputImprovementForStage(stage.first,&preImprovement->record[stage.first]);
            else {
                preImprovement->record[stage.first] = {};
                this->analyzeMaxNetThroughputImprovementForStage(stage.first, &preImprovement->record[stage.first]);
            }

        }


    }
    void analyzeMaxShuffleRateImprovementForStages(shared_ptr<QueryRecorder> preImprovement)
    {

        map<int,map<shared_ptr<ClusterNode>,vector<double>>> newImprovements;
        for(auto stage : this->stagesNodeTaskMapSnap)
        {
            if(preImprovement->record.count(stage.first) > 0)
                this->analyzeMaxShuffleRateImprovementForStage(stage.first,&preImprovement->record[stage.first]);
            else {
                preImprovement->record[stage.first] = {};
                this->analyzeMaxShuffleRateImprovementForStage(stage.first,&preImprovement->record[stage.first]);
            }
        }

    }


    void analyzeMaxCpuRateImprovementForStages(pair<shared_ptr<QueryRecorder>,shared_ptr<QueryRecorder>> &preImprovement)
    {

        map<int,map<shared_ptr<ClusterNode>,vector<double>>> newImprovements;
        for(auto stage : this->stagesNodeTaskMapSnap)
        {
            if(preImprovement.first->record.count(stage.first) > 0)
                this->analyzeMaxCpuRateImprovementForStage(stage.first,&preImprovement.first->record[stage.first],&preImprovement.second->record[stage.first]);
            else {
                preImprovement.first->record[stage.first] = {};
                this->analyzeMaxCpuRateImprovementForStage(stage.first,&preImprovement.first->record[stage.first],&preImprovement.second->record[stage.first]);
            }
        }

    }

    void analyzeMaxNetThroughputImprovementForStage(int stageId,map<shared_ptr<ClusterNode>,shared_ptr<smallWindow>> *preImprovement)
    {
        map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>> stageTasks = (*this->stagesNodeTaskMapSnap[stageId]);

        set<shared_ptr<ClusterNode>> selectedNodes;

        for(auto node : stageTasks)
            selectedNodes.insert(node.first);


        auto allTasksOnSelectedNodes = this->clusterContext->getTaskInfosByNodes(selectedNodes);

        map<shared_ptr<ClusterNode>,double> totalOutputThroughputOnEachNode;
        map<shared_ptr<ClusterNode>,double> targetQueryOutputThroughputOnEachNode;

        for(auto node : allTasksOnSelectedNodes)
        {
            totalOutputThroughputOnEachNode[node.first] = node.second->getLongTotalOutputThroughput();
        }


        for(auto node : stageTasks)
        {
            double totalQueryTasksNetUsageByNode = 0.0;
            for(auto task : node.second)
            {
                totalQueryTasksNetUsageByNode+=task->getTaskInfoFetcher()->getLongAvgThroughputBytes();
            }
            targetQueryOutputThroughputOnEachNode[node.first] = totalQueryTasksNetUsageByNode;
        }

        map<shared_ptr<ClusterNode>,double> improvementsOfStage;

        for(auto node : totalOutputThroughputOnEachNode)
        {
            double ratio = targetQueryOutputThroughputOnEachNode[node.first]/totalOutputThroughputOnEachNode[node.first];
           // spdlog::info(to_string(targetQueryOutputThroughputOnEachNode[node.first])+"/"+ to_string(totalOutputThroughputOnEachNode[node.first]));
            double netWidthOccupy = node.first->getCurrentNetTransRate() * ratio;
            if(netWidthOccupy > 0)
                improvementsOfStage[node.first] = node.first->getRemainingTransThroughput()/netWidthOccupy;
            else
                improvementsOfStage[node.first] = 0;
        }


   //     spdlog::info("For stage : "+ to_string(stageId));
  //      for(auto node : improvementsOfStage)
  //      {
  //          spdlog::info("On node" + node.first->getNodeLocation()+" improve "+ to_string(node.second));
 //       }

        for(auto node : improvementsOfStage)
        {
            if((*preImprovement).count(node.first) > 0)
            {
                (*preImprovement)[node.first]->addValue(node.second);
            }
            else
            {
                (*preImprovement)[node.first] = make_shared<smallWindow>(5);
                (*preImprovement)[node.first]->addValue(node.second);
            }

        }
    }

    double getRemainingCpuUsageOfNode(shared_ptr<ClusterNode> node)
    {
        double cur = node->getCurrentCpuUsage();
        double max = ((double)node->getCoreNums()) * 100;
        if(max-cur < 0)
            return 0;
        return max -cur;
    }
    void analyzeMaxShuffleRateImprovementForStage(int stageId,map<shared_ptr<ClusterNode>,shared_ptr<smallWindow>> *preImprovement)
    {

        map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>> stageTasks = (*this->stagesNodeTaskMapSnap[stageId]);

        map<shared_ptr<ClusterNode>,double> improvements;
        for(auto node : stageTasks)
        {
            double totalShuffleCpuUsage = 0.0;
            for(auto task : node.second)
            {
                totalShuffleCpuUsage+=task->getTaskInfoFetcher()->getAvgShuffleCpuUsage();
            }
            if(totalShuffleCpuUsage == 0)
                improvements[node.first] = 0;
            else
                improvements[node.first] = this->getRemainingCpuUsageOfNode(node.first)/totalShuffleCpuUsage;
        }


    //    spdlog::info("For stage : "+ to_string(stageId));
    //    for(auto node : improvements)
    //    {
    //        spdlog::info("On node" + node.first->getNodeLocation()+" improve "+ to_string(node.second));
    //    }
        for(auto node : improvements)
        {
            if((*preImprovement).count(node.first) > 0)
            {
                (*preImprovement)[node.first]->addValue(improvements[node.first]);
            }
            else
            {
                (*preImprovement)[node.first] = make_shared<smallWindow>();
            }
        }

    }

    void analyzeMaxCpuRateImprovementForStage(int stageId,map<shared_ptr<ClusterNode>,shared_ptr<smallWindow>> *preImprovement,map<shared_ptr<ClusterNode>,shared_ptr<smallWindow>> *preImprovementByThreads)
    {

        map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>> stageTasks = (*this->stagesNodeTaskMapSnap[stageId]);

        map<shared_ptr<ClusterNode>,double> improvements;

        map<shared_ptr<ClusterNode>,double> improvementsByThreads;
        for(auto node : stageTasks)
        {
            double totalCpuUsage = 0.0;
            for(auto task : node.second)
            {
                totalCpuUsage+=task->getTaskInfoFetcher()->getAvgCpuUsage();
            }
            if(totalCpuUsage == 0) {
                improvements[node.first] = 0;
                improvementsByThreads[node.first] = 0;
            }
            else {
                improvements[node.first] = this->getRemainingCpuUsageOfNode(node.first) / totalCpuUsage;
                improvementsByThreads[node.first] = this->queryToPredict->getRemainingTaskCpuUsageOfStageByTaskThreadNums(stageId);
            }
        }


        //    spdlog::info("For stage : "+ to_string(stageId));
        //    for(auto node : improvements)
        //    {
        //        spdlog::info("On node" + node.first->getNodeLocation()+" improve "+ to_string(node.second));
        //    }
        for(auto node : improvements)
        {
            if((*preImprovement).count(node.first) > 0)
            {
                (*preImprovement)[node.first]->addValue(improvements[node.first]);
                (*preImprovementByThreads)[node.first]->addValue(improvementsByThreads[node.first]);
            }
            else
            {
                (*preImprovement)[node.first] = make_shared<smallWindow>(5);
                (*preImprovementByThreads)[node.first] = make_shared<smallWindow>(5);
            }
        }

    }

    vector<shared_ptr<StageTaskMaxImproveRatioResult>> analyzeMaxStageCpuUsageImprovementOnCurrentCluster()
    {
        vector<shared_ptr<StageTaskMaxImproveRatioResult>> results;
        for(auto stage : this->stagesNodeTaskMapSnap)
        {
            auto ratioNode = this->maxStageCpuUsageImprovementOnCurrentCluster(stage.first);
            auto ratioThread = this->maxStageCpuUsageByThreadNumsImprovementOnCurrentCluster(stage.first);
            results.push_back(make_shared<StageTaskMaxImproveRatioResult>(stage.first,ratioNode,ratioThread));
        }
        return results;

    }

    double maxStageCpuUsageImprovementOnCurrentCluster(int stageId)
    {

        double avgTaskCpuUsage = this->queryToPredict->getSingleTaskCpuUsageOfStage(stageId);
        double ratioByTaskThreadNums = this->queryToPredict->getRemainingTaskCpuUsageOfStageByTaskThreadNums(stageId);
        auto result = this->nodesManager->getRemainingCpuUsagesImprovementRatio(avgTaskCpuUsage);

        return result;
    }


    double maxStageCpuUsageByThreadNumsImprovementOnCurrentCluster(int stageId)
    {

        double avgTaskCpuUsage = this->queryToPredict->getSingleTaskCpuUsageOfStage(stageId);
        double ratioByTaskThreadNums = this->queryToPredict->getRemainingTaskCpuUsageOfStageByTaskThreadNums(stageId);
        return ratioByTaskThreadNums;
    }


    void traverseToFindBottleneck(set<shared_ptr<StageExecutionAndScheduler>> &result,
                                  shared_ptr<StageExecutionAndScheduler>stage,vector<shared_ptr<StageExecutionAndScheduler>> &stages)
    {
        auto trend = stage->getStageExecution()->getStageBufferSizeChangingTrend();


        if(trend[0] == 1 && trend[3] != 1) {
            //Output buffer expand! Exchange buffer not shrink!

            if(this->queryToPredict->getScheduler()->getLeftChildStagesSetForStage(to_string(stage->getStageExecution()->getStageId().getId())).empty()){

                if(!this->queryToPredict->getScheduler()->isRootStage(stage->getStageExecution()))
                    result.insert(stage);
            }
            else {
                shared_ptr<StageExecutionAndScheduler> back = stages.back();
                stages.pop_back();
                traverseToFindBottleneck(result, back, stages);
            }
        }
      //  else if(trend[1] == 1) {
    //        //"Output buffer shrink!
    //    }
     //   else if(trend[2] == 1) {
   //         //Exchange buffer expand!
    //    }
        else if(trend[3] == 1 && !this->queryToPredict->getScheduler()->isRootStage(stage->getStageExecution())) {
            //Exchange buffer shrink!

            result.insert(stage);

            auto childs = this->queryToPredict->getScheduler()->getLeftChildStagesSetForStage
                    (to_string(stage->getStageExecution()->getStageId().getId()));

            for (auto it = stages.begin(); it != stages.end();) {
                if (childs.contains(*it))
                    it = stages.erase(it);
                else
                    ++it;
            }
        }

        if(!stages.empty()) {
            auto back = stages.back();
            stages.pop_back();
            traverseToFindBottleneck(result, back, stages);
        }
    }


    vector<shared_ptr<QueryBottleneckResult>> analyzeQueryBottleneck()
    {

        if(!this->queryToPredict->isQueryStart())
            return {};

        vector<StageExecutionAndScheduler> stages;

        stages = this->queryToPredict->getScheduler()->getStageExeSchedulers();

        vector<shared_ptr<StageExecutionAndScheduler>> stageExes;

        for(auto stage : stages)
        {
            bitset<4> re = stage.getStageExecution()->getStageBufferSizeChangingTrend();
            stageExes.push_back(make_shared<StageExecutionAndScheduler>(stage.getStageExecution(),stage.getStageLinkage(),stage.getStageScheduler()));

        }

        set<shared_ptr<StageExecutionAndScheduler>> result;
        auto back = stageExes.back();
        stages.pop_back();
        traverseToFindBottleneck(result,back,stageExes);

        vector<shared_ptr<QueryBottleneckResult>> finalResults;

        for(auto re : result) {

            finalResults.push_back(make_shared<QueryBottleneckResult>(re->getStageExecution()->getStageId().getId(),QueryBottleneckResult::compute));
        }

        auto recbottlenecks = this->netRecBottleneckResult[this->queryToPredict->getSession()->getQueryId()];
        auto transbottlenecks = this->netTransBottleneckResult[this->queryToPredict->getSession()->getQueryId()];

        if(!recbottlenecks.empty())
        {
            for(auto bottleneck : recbottlenecks)
            {
                finalResults.push_back(make_shared<QueryBottleneckResult>(bottleneck->getStageId(),QueryBottleneckResult::net_rec));
            }
        }
        if(!transbottlenecks.empty())
        {
            for(auto bottleneck : transbottlenecks)
            {
                finalResults.push_back(make_shared<QueryBottleneckResult>(bottleneck->getStageId(),QueryBottleneckResult::net_out));
            }
        }

        return finalResults;

    }

    void findBottlenecksForStageExtern(string queryId,int stageId,vector<shared_ptr<CpuBottleneckResult>> &cpu,vector<shared_ptr<NetBottleneckResult>> &nettrans,vector<shared_ptr<NetBottleneckResult>> &netrec)
    {
        auto cres = this->cpuBottleneckResult[queryId];
        auto ntres = this->netTransBottleneckResult[queryId];
        auto nrres = this->netRecBottleneckResult[queryId];

        vector<shared_ptr<CpuBottleneckResult>> cpuout;
        vector<shared_ptr<NetBottleneckResult>> nettransout;
        vector<shared_ptr<NetBottleneckResult>> netrecout;

        for(auto result : cres)
            if(result->getStageId() == stageId)
                cpu.push_back(result);

        for(auto result : ntres)
            if(result->getStageId() == stageId)
                nettrans.push_back(result);

        for(auto result : nrres)
            if(result->getStageId() == stageId)
                netrec.push_back(result);


    }


    bool isStageCanImprove(string queryId,int stageId,int n, int &max)
    {
        auto stagesMaxTasks = this->stageTaskMaxImproveRatioResult[queryId];
        for(auto s : stagesMaxTasks)
        {
            if(s->getStageId() == stageId)
            {
                max = s->getMaxRatio()+1;
                if(s->getMaxRatio()+1 > n) {
                    return true;
                }
                else
                    return false;
            }
        }

        return false;
    }




    string getQueryBottleneckStagesExtern(string queryId)
    {
        string result;
        if(this->queries->count(queryId) == 0)
            return "Can not find the query!";
        else
        {
            if((*this->queries)[queryId]->isQueryFinished())
                return "Query is finished!";

            if(!(*this->queries)[queryId]->isQueryStart())
                return "Query is planning!";


        }
        if(this->queryBottleneckResult.count(queryId) > 0) {
            auto re = this->queryBottleneckResult[queryId];
            for(auto r : re)
            {
                result.append("Stage " + to_string(r->getStageId()) + " is bottleneck! Because of the "+r->getBottleneckType()+"\n");
            }
            return result;
        }
        else
            return "No bottlenecks have been identified for this query.";
    }

    string getQueryBottleneckStagesAndAnalyzeExtern(string queryId,int factor)
    {
        string result;
        if(this->queries->count(queryId) == 0)
            return "Can not find the query!";
        else
        {
            if((*this->queries)[queryId]->isQueryFinished())
                return "Query is finished!";

            if(!(*this->queries)[queryId]->isQueryStart())
                return "Query is planning!";


        }
        if(this->queryBottleneckResult.count(queryId) > 0) {
            auto re = this->queryBottleneckResult[queryId];
            for(auto r : re)
            {
                result.append("Stage " + to_string(r->getStageId()) + " is bottleneck! ");
                auto analyze = this->getStageImprovementPredictionInfosExtern(queryId,r->getStageId(),factor);
                result.append(analyze);

            }



            return result;
        }
        else
            return "No bottlenecks have been identified for this query.";
    }

    void testStageImprovementPredictionExtern(string queryId,int stageId,int n)
    {

        double preTime = 0.0;
        auto targetStage = (*this->queries)[queryId]->getScheduler();

        //find the left tablescan stage to get the remaining time
        auto tableScanStage = targetStage->findRootTableScanStageForStage(to_string(stageId));
        double remainingTime = tableScanStage->getStageExecution()->getRemainingTime()/1000;

        map<int,long> stageBytes = targetStage->getStageBuildChildsTotalBytes(to_string(stageId));
        map<shared_ptr<ClusterNode>,double> timeConsumes;
        double MaxRebuildTime = 0;
        for(auto stage : stageBytes)
        {
            spdlog::info("Build stages "+to_string(stage.first) +" have "+to_string(stage.second)+" bytes to trans");
            auto nodes = targetStage->getStageExecutionAndSchedulerByStagId(stage.first)->getStageExecution()->getCurrentStageNodes();

            double eachNodeDataVolume = stage.second/nodes.size();

            for(auto node : nodes)
            {
                if(timeConsumes.count(node) == 0)
                {
                    timeConsumes[node] = eachNodeDataVolume/1000/node->getMaxNetSpeed();
                }
                else
                    timeConsumes[node] += eachNodeDataVolume/1000/node->getMaxNetSpeed();
            }
        }
        for(auto time : timeConsumes)
        {
            spdlog::info("Stage rebuild data trans "+time.first->getNodeLocation() +" spend "+to_string(time.second)+"s");
            if(time.second > MaxRebuildTime)
                MaxRebuildTime = time.second;
        }

        spdlog::info("Min stage rebuild data transmission time is " + to_string(n*MaxRebuildTime));


        auto targetStageObj = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution();
        //get the hashtable build time
        double stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildTimeofTasks();

        preTime = remainingTime - stageTaskMaxBuildTime;
        stageTaskMaxBuildTime = stageTaskMaxBuildTime/1000;

        spdlog::info("Max stage task hash build time is " + to_string(stageTaskMaxBuildTime));

        //first let we figure out is there enough cpu resources to create new tasks for this stage
        int maxCpuGet = 0;
        if(this->isStageCanImprove(queryId,stageId,n,maxCpuGet)) spdlog::info("have enough cpu resources to add task for this stage !");

        vector<shared_ptr<CpuBottleneckResult>> cpu;
        vector<shared_ptr<NetBottleneckResult>> nettrans;
        vector<shared_ptr<NetBottleneckResult>> netrec;
        findBottlenecksForStageExtern(queryId,stageId,cpu,nettrans,netrec);

        //then we figure out is there net rec bottleneck on this stage
        for(auto rec : netrec)
        {
            if(rec->getStageId() == stageId)
            {
                //preTime = remainingTime;
                spdlog::info("this stage have net rec bottleneck! Cur:"+ to_string(rec->getCurValue())+" Max:" +
                             to_string(rec->getMaxValue()));
            }
        }

        //then we figure out if the left upstream stage's net and cpu usage can be boosted by n

        auto childStages = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageLinkage();
        if(!childStages->getChildStages().empty()) {
            auto leftChild = childStages->getChildStages()[0];

            if(!leftChild->isBufferNeedPartitioning())
            {
                stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildComputingTimeofTasks()/1000+MaxRebuildTime;
            }

            findBottlenecksForStageExtern(queryId,leftChild->getStageId().getId(),cpu,nettrans,netrec);

            auto netIm = this->query_stage_node_net_improvement[queryId];
            auto shuffleIm = this->query_stage_node_shuffle_improvement[queryId];
            auto cpuIm = this->query_stage_node_cpu_improvement[queryId];

            double netImprove = 0;
            double shuffleImprove = 0;
            double cpuImprove = 0;
            double cpuImproveByThreads = 0;
            if(netIm != NULL && shuffleIm != NULL && cpuIm.first != NULL) {
                 netImprove = netIm->getStageMinImprovement(leftChild->getStageId().getId());
                 shuffleImprove = shuffleIm->getStageMinImprovement(leftChild->getStageId().getId());
                 cpuImprove = cpuIm.first->getStageMinImprovement(leftChild->getStageId().getId());
                 cpuImproveByThreads = cpuIm.second->getStageAVGImprovement(leftChild->getStageId().getId());
            }

            for(auto trans : nettrans)
            {
                if(trans->getStageId() == leftChild->getStageId().getId())
                {
                    spdlog::info("this child stage have net trans bottleneck! Cur:"+ to_string(trans->getCurValue())+" Max:" +
                                                                                                               to_string(trans->getMaxValue()));
                }
            }

            if (netImprove + 1 > n)
                spdlog::info("child stage network transmission bandwidth can be expanded by " + to_string(n) + ", max is " +
                             to_string(netImprove + 1));
            else
                spdlog::info("child stage network transmission bandwidth cannot be expanded by " + to_string(n) + ", max is " +
                             to_string(netImprove + 1));

            if (cpuImprove + 1 > n)
                spdlog::info(
                        "child stage cpu rate can be expanded by " + to_string(n) + ", max is" + to_string(cpuImprove + 1));
            else
                spdlog::info("child stage cpu rate cannot be expanded by " + to_string(n) + ", max is" +
                             to_string(cpuImprove + 1));

            if (cpuImproveByThreads + 1 > n)
                spdlog::info(
                        "child stage cpu rate by DOP can be expanded by " + to_string(n) + ", max is" + to_string(cpuImproveByThreads + 1));
            else
                spdlog::info("child stage cpu rate by DOP cannot be expanded by " + to_string(n) + ", max is" +
                             to_string(cpuImproveByThreads + 1));

            if (shuffleImprove + 1 > n)
                spdlog::info(
                        "child stage shuffle rate can be expanded by " + to_string(n) + ", max is" + to_string(shuffleImprove + 1));
            else
                spdlog::info("child stage shuffle rate cannot be expanded by " + to_string(n) + ", max is" +
                             to_string(shuffleImprove + 1));


            double minFactor = 9999999.99;
            if(netImprove + 1 < minFactor && netImprove + 1 > 1)
                minFactor = netImprove+1;

            if(leftChild->isHavingAdaptiveDOP()){
                if(cpuImprove + 1 < minFactor && cpuImprove + 1 > 1)
                    minFactor = cpuImprove+1;
            }
            else
            {
                double minCPUFactor = 9999999999;
                if(cpuImprove + 1 > 1 && cpuImprove + 1 < minCPUFactor) {
                    minCPUFactor = cpuImprove + 1;
                }
                if(cpuImproveByThreads + 1 > 1 && cpuImproveByThreads + 1 < minCPUFactor) {
                    minCPUFactor = cpuImproveByThreads + 1;
                }
                if(minCPUFactor < minFactor)
                    minFactor = minCPUFactor;
            }

            if(shuffleImprove + 1 < minFactor && shuffleImprove + 1 > 1)
                minFactor = shuffleImprove+1;
            if(n < minFactor)
                minFactor = n;
            if(minFactor == 9999999)
                minFactor = 1;


            preTime = (remainingTime-stageTaskMaxBuildTime)/minFactor;
            preTime += stageTaskMaxBuildTime;
            spdlog::info("The predicted remaining time is ("+ to_string(remainingTime)+"-"+to_string(stageTaskMaxBuildTime)+")/"+
                                                                                                                            to_string(minFactor)+"+"+to_string(stageTaskMaxBuildTime)+"="+to_string(preTime));
            if(n*MaxRebuildTime > preTime)
            {
                spdlog::info("Build data transmission time is greater than predicted time!");
            }

        }
        else
        {
            preTime = remainingTime;
            spdlog::info("cannot change table scan stage dop!");


        }

    }


    string getStageImprovementPredictionExtern(string queryId,int stageId,int n)
    {

        if((*this->queries).count(queryId) == 0)
            return "Query is finished!";

        if (!(*this->queries)[queryId]->isQueryStart()) {
            return "Query is planning!";
        }

        string result;
        double preTime = 0.0;
        auto targetStage = (*this->queries)[queryId]->getScheduler();

        //find the left tablescan stage to get the remaining time
        auto tableScanStage = targetStage->findRootTableScanStageForStage(to_string(stageId));
        double remainingTime = tableScanStage->getStageExecution()->getRemainingTime()/1000;

        map<int,long> stageBytes = targetStage->getStageBuildChildsTotalBytes(to_string(stageId));
        map<shared_ptr<ClusterNode>,double> timeConsumes;
        double MaxRebuildTime = 0;
        for(auto stage : stageBytes)
        {
            result.append("Build stages "+to_string(stage.first) +" have "+to_string(stage.second)+" bytes to trans\n");
            auto nodes = targetStage->getStageExecutionAndSchedulerByStagId(stage.first)->getStageExecution()->getCurrentStageNodes();

            double eachNodeDataVolume = stage.second/nodes.size();

            for(auto node : nodes)
            {
                if(timeConsumes.count(node) == 0)
                {
                    timeConsumes[node] = eachNodeDataVolume/1000/node->getMaxNetSpeed();
                }
                else
                    timeConsumes[node] += eachNodeDataVolume/1000/node->getMaxNetSpeed();
            }
        }
        for(auto time : timeConsumes)
        {
            result.append("Stage rebuild data trans "+time.first->getNodeLocation() +" spend "+to_string(time.second)+"s\n");
            if(time.second > MaxRebuildTime)
                MaxRebuildTime = time.second;
        }

        result.append("Min stage rebuild data transmission time is " + to_string(n*MaxRebuildTime)+"\n");

        auto targetStageObj = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution();
        //get the hashtable build time
        double stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildTimeofTasks();

        preTime = remainingTime - stageTaskMaxBuildTime;
        stageTaskMaxBuildTime = stageTaskMaxBuildTime/1000;

        result.append("Max stage task hash build time is " + to_string(stageTaskMaxBuildTime)+"\n");

        //first let we figure out is there enough cpu resources to create new tasks for this stage
        int maxCpuGet = 0;
        if(this->isStageCanImprove(queryId,stageId,n,maxCpuGet)) result.append("have enough cpu resources to add task for this stage !\n");

        vector<shared_ptr<CpuBottleneckResult>> cpu;
        vector<shared_ptr<NetBottleneckResult>> nettrans;
        vector<shared_ptr<NetBottleneckResult>> netrec;
        findBottlenecksForStageExtern(queryId,stageId,cpu,nettrans,netrec);

        //then we figure out is there net rec bottleneck on this stage
        for(auto rec : netrec)
        {
            if(rec->getStageId() == stageId)
            {
                //preTime = remainingTime;
                result.append("this stage have net rec bottleneck! Cur:"+ to_string(rec->getCurValue())+" Max:" +
                             to_string(rec->getMaxValue())+"\n");
            }
        }

        //then we figure out if the left upstream stage's net and cpu usage can be boosted by n

        auto childStages = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageLinkage();
        if(!childStages->getChildStages().empty()) {
            auto leftChild = childStages->getChildStages()[0];

            if(!leftChild->isBufferNeedPartitioning())
            {
                stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildComputingTimeofTasks()/1000+MaxRebuildTime;
            }

            findBottlenecksForStageExtern(queryId,leftChild->getStageId().getId(),cpu,nettrans,netrec);

            auto netIm = this->query_stage_node_net_improvement[queryId];
            auto shuffleIm = this->query_stage_node_shuffle_improvement[queryId];
            auto cpuIm = this->query_stage_node_cpu_improvement[queryId];

            double netImprove = 0;
            double shuffleImprove = 0;
            double cpuImprove = 0;
            double cpuImproveByThreads = 0;
            if(netIm != NULL && shuffleIm != NULL && cpuIm.first != NULL) {
                netImprove = netIm->getStageMinImprovement(leftChild->getStageId().getId());
                shuffleImprove = shuffleIm->getStageMinImprovement(leftChild->getStageId().getId());
                cpuImprove = cpuIm.first->getStageMinImprovement(leftChild->getStageId().getId());
                cpuImproveByThreads = cpuIm.second->getStageAVGImprovement(leftChild->getStageId().getId());
            }

            for(auto trans : nettrans)
            {
                if(trans->getStageId() == leftChild->getStageId().getId())
                {
                    result.append("this child stage have net trans bottleneck! Cur:"+ to_string(trans->getCurValue())+" Max:" +
                                 to_string(trans->getMaxValue())+"\n");
                }
            }

            if (netImprove + 1 > n)
                result.append("child stage network transmission bandwidth can be expanded by " + to_string(n) + ", max is " +
                             to_string(netImprove + 1)+"\n");
            else
                result.append("child stage network transmission bandwidth cannot be expanded by " + to_string(n) + ", max is " +
                             to_string(netImprove + 1)+"\n");

            if (cpuImprove + 1 > n)
                result.append(
                        "child stage cpu rate can be expanded by " + to_string(n) + ", max is" + to_string(cpuImprove + 1));
            else
                result.append("child stage cpu rate cannot be expanded by " + to_string(n) + ", max is" +
                             to_string(cpuImprove + 1)+"\n");


            if (cpuImproveByThreads + 1 > n)
                result.append(
                        "child stage cpu rate can be expanded by " + to_string(n) + ", max is" + to_string(cpuImproveByThreads + 1));
            else {
                result.append("LOW DOP! child stage cpu rate cannot be expanded by " + to_string(n) + ", max is" +
                              to_string(cpuImproveByThreads + 1)+"\n");
            }


            if (shuffleImprove + 1 > n)
                result.append(
                        "child stage shuffle rate can be expanded by " + to_string(n) + ", max is" + to_string(shuffleImprove + 1));
            else
                result.append("child stage shuffle rate cannot be expanded by " + to_string(n) + ", max is" +
                             to_string(shuffleImprove + 1)+"\n");


            double minFactor = 9999999.99;
            if(netImprove + 1 < minFactor && netImprove + 1 > 1)
                minFactor = netImprove+1;
            if(leftChild->isHavingAdaptiveDOP()){
                if(cpuImprove + 1 < minFactor && cpuImprove + 1 > 1)
                    minFactor = cpuImprove+1;
            }
            else
            {
                double minCPUFactor = 9999999999;
                if(cpuImprove + 1 > 1 && cpuImprove + 1 < minCPUFactor) {
                    minCPUFactor = cpuImprove + 1;
                }
                if(cpuImproveByThreads + 1 > 1 && cpuImproveByThreads + 1 < minCPUFactor) {
                    minCPUFactor = cpuImproveByThreads + 1;
                }
                if(minCPUFactor < minFactor)
                    minFactor = minCPUFactor;
            }
            if(shuffleImprove + 1 < minFactor && shuffleImprove + 1 > 1)
                minFactor = shuffleImprove+1;
            if(n < minFactor)
                minFactor = n;
            if(minFactor == 9999999)
                minFactor = 1;


            preTime = (remainingTime-stageTaskMaxBuildTime)/minFactor;
            preTime += stageTaskMaxBuildTime;
            result.append("The predicted remaining time is ("+ to_string(remainingTime)+"-"+to_string(stageTaskMaxBuildTime)+")/"+
                         to_string(minFactor)+"+"+to_string(stageTaskMaxBuildTime)+"="+to_string(preTime)+"\n");
            if(n*MaxRebuildTime > preTime)
            {
                result.append("Build data transmission time is greater than predicted time!\n");
            }

        }
        else
        {
            preTime = remainingTime;
            result.append("cannot change table scan stage dop!\n");

        }
        return result;

    }
    string getStageImprovementPredictionInfosExtern(string queryId,int stageId,double n)
    {

        if((*this->queries).count(queryId) == 0)
            return "Query is finished!";

        if (!(*this->queries)[queryId]->isQueryStart()) {
            return "Query is planning!";
        }


        string prediction;

        string reasons;



        double preTime = 0.0;
        auto targetStage = (*this->queries)[queryId]->getScheduler();

        //find the left tablescan stage to get the remaining time
        auto tableScanStage = targetStage->findRootTableScanStageForStage(to_string(stageId));
        double remainingTime = tableScanStage->getStageExecution()->getRemainingTime()/1000;

        map<int,long> stageBytes = targetStage->getStageBuildChildsTotalBytes(to_string(stageId));
        map<shared_ptr<ClusterNode>,double> timeConsumes;
        double MaxRebuildTime = 0;
        for(auto stage : stageBytes)
        {
            //result.append("Build stages "+to_string(stage.first) +" have "+to_string(stage.second)+" bytes to trans\n");
            auto nodes = targetStage->getStageExecutionAndSchedulerByStagId(stage.first)->getStageExecution()->getCurrentStageNodes();

            double eachNodeDataVolume = stage.second/nodes.size();

            for(auto node : nodes)
            {
                if(timeConsumes.count(node) == 0)
                {
                    timeConsumes[node] = eachNodeDataVolume/1000/node->getMaxNetSpeed();
                }
                else
                    timeConsumes[node] += eachNodeDataVolume/1000/node->getMaxNetSpeed();
            }
        }
        for(auto time : timeConsumes)
        {
           // result.append("Stage rebuild data trans "+time.first->getNodeLocation() +" spend "+to_string(time.second)+"s\n");
           // if(time.second > MaxRebuildTime)
           //     MaxRebuildTime = time.second;
        }

       // result.append("Min stage rebuild data transmission time is " + to_string(n*MaxRebuildTime)+"\n");

       auto targetStageObj = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution();
        //get the hashtable build time
        double stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildTimeofTasks();

        preTime = remainingTime - stageTaskMaxBuildTime;
        stageTaskMaxBuildTime = stageTaskMaxBuildTime/1000;

       // result.append("Max stage task hash build time is " + to_string(stageTaskMaxBuildTime)+"\n");

        //first let we figure out is there enough cpu resources to create new tasks for this stage
        int maxCpuGet = 0;
        if(!this->isStageCanImprove(queryId,stageId,n,maxCpuGet)) {
            reasons.append("#Lack of cpu resources to create tasks!");
        }

        vector<shared_ptr<CpuBottleneckResult>> cpu;
        vector<shared_ptr<NetBottleneckResult>> nettrans;
        vector<shared_ptr<NetBottleneckResult>> netrec;
        findBottlenecksForStageExtern(queryId,stageId,cpu,nettrans,netrec);

        //then we figure out is there net rec bottleneck on this stage
        for(auto rec : netrec)
        {
            if(rec->getStageId() == stageId)
            {
                //preTime = remainingTime;
                reasons.append("#NIC receive bottleneck!");
            }
        }

        //then we figure out if the left upstream stage's net and cpu usage can be boosted by n

        auto childStages = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageLinkage();
        if(!childStages->getChildStages().empty()) {
            auto leftChild = childStages->getChildStages()[0];



            if(!leftChild->isBufferNeedPartitioning())
            {
                stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildComputingTimeofTasks()/1000+MaxRebuildTime;
            }


            findBottlenecksForStageExtern(queryId,leftChild->getStageId().getId(),cpu,nettrans,netrec);

            auto netIm = this->query_stage_node_net_improvement[queryId];
            auto shuffleIm = this->query_stage_node_shuffle_improvement[queryId];
            auto cpuIm = this->query_stage_node_cpu_improvement[queryId];

            double netImprove = 0;
            double shuffleImprove = 0;
            double cpuImprove = 0;
            double cpuImproveByThreads = 0;
            if(netIm != NULL && shuffleIm != NULL && cpuIm.first != NULL) {
                netImprove = netIm->getStageMinImprovement(leftChild->getStageId().getId());
                shuffleImprove = shuffleIm->getStageMinImprovement(leftChild->getStageId().getId());
                cpuImprove = cpuIm.first->getStageMinImprovement(leftChild->getStageId().getId());
                cpuImproveByThreads = cpuIm.second->getStageAVGImprovement(leftChild->getStageId().getId());
            }

            for(auto trans : nettrans)
            {
                if(trans->getStageId() == leftChild->getStageId().getId())
                {
                   // reasons.append("upstream net trans bottleneck!");
                }
            }

            if (netImprove + 1 > n)
                ;
            else {
               // reasons.append("#Upstream NIC transmission bottleneck! Max improvement:" +
                  //             to_string(netImprove + 1));

                reasons.append(" #Upstream NIC transmission bottleneck!");
            }
            if (cpuImprove + 1 > n)
               ;
            else {
                // reasons.append("#Upstream lack of cpu resources! Max improvement:" +
                //             to_string(cpuImprove + 1));

                reasons.append(" #Upstream lack of cpu resources!");
            }
            if (cpuImproveByThreads + 1 > n)
                ;
            else {
                // reasons.append("#Upstream lack of cpu resources! Max improvement:" +
                //             to_string(cpuImprove + 1));

                reasons.append(" #Upstream DOP is low!");
            }

            if (shuffleImprove + 1 > n)
              ;
            else {
                if(shuffleImprove + 1 > 1) {

                    reasons.append(" #Upstream lack of cpu resources for shuffle");
                    //reasons.append("#Upstream lack of cpu resources for shuffle, Max improvement:" +
                      //             to_string(shuffleImprove + 1));
                }
            }

            double minFactor = 9999999.99;
            if(netImprove + 1 < minFactor && netImprove + 1 > 1)
                minFactor = netImprove+1;

            if(leftChild->isHavingAdaptiveDOP()){
                if(cpuImprove + 1 < minFactor && cpuImprove + 1 > 1)
                    minFactor = cpuImprove+1;
            }
            else
            {
                double minCPUFactor = 9999999999;
                if(cpuImprove + 1 > 1 && cpuImprove + 1 < minCPUFactor) {
                    minCPUFactor = cpuImprove + 1;
                }
                if(cpuImproveByThreads + 1 > 1 && cpuImproveByThreads + 1 < minCPUFactor) {
                    minCPUFactor = cpuImproveByThreads + 1;
                }
                if(minCPUFactor < minFactor)
                    minFactor = minCPUFactor;
            }


            if(shuffleImprove + 1 < minFactor && shuffleImprove + 1 > 1)
                minFactor = shuffleImprove+1;
            if(n < minFactor)
                minFactor = n;
            if(minFactor == 9999999)
                minFactor = 1;


            if(minFactor != n)
            {
                prediction.append("Can't accelerate "+ to_string(n)+" times! Max is "+ to_string(minFactor)+" !\n");
            }


            preTime = (remainingTime-stageTaskMaxBuildTime)/minFactor;
            preTime += stageTaskMaxBuildTime;

            prediction.append("Predicted time: ["+to_string(preTime)+"] ");
            if(n*MaxRebuildTime > preTime)
            {
                reasons.append(" !!Warning: Data transmission time is greater than predicted time!");
            }

        }
        else
        {
            preTime = remainingTime;
            reasons.append(" **Need upgrade the hardware configuration.");

        }
        if(!prediction.empty())
            prediction = "\n"+prediction;
        return prediction+"\nDetails:"+reasons+"\n";

    }



    string getStagePredictedTime(string queryId,int stageId,double n)
    {

        if((*this->queries).count(queryId) == 0)
            return "---";

        if (!(*this->queries)[queryId]->isQueryStart()) {
            return "---";
        }


        string prediction;

        string reasons;

        string computingMethods;

        double preTime;
        auto targetStage = (*this->queries)[queryId]->getScheduler();

        //find the left tablescan stage to get the remaining time
        auto tableScanStage = targetStage->findRootTableScanStageForStage(to_string(stageId));
        double remainingTime = tableScanStage->getStageExecution()->getRemainingTime()/1000;

        map<int,long> stageBytes = targetStage->getStageBuildChildsTotalBytes(to_string(stageId));
        map<shared_ptr<ClusterNode>,double> timeConsumes;
        double MaxRebuildTime = 0;
        for(auto stage : stageBytes)
        {
            //result.append("Build stages "+to_string(stage.first) +" have "+to_string(stage.second)+" bytes to trans\n");
            auto nodes = targetStage->getStageExecutionAndSchedulerByStagId(stage.first)->getStageExecution()->getCurrentStageNodes();

            double eachNodeDataVolume = stage.second/nodes.size();

            for(auto node : nodes)
            {
                if(timeConsumes.count(node) == 0)
                {
                    timeConsumes[node] = eachNodeDataVolume/1000/node->getMaxNetSpeed();
                }
                else
                    timeConsumes[node] += eachNodeDataVolume/1000/node->getMaxNetSpeed();
            }
        }
        for(auto time : timeConsumes)
        {
            // result.append("Stage rebuild data trans "+time.first->getNodeLocation() +" spend "+to_string(time.second)+"s\n");
            // if(time.second > MaxRebuildTime)
            //     MaxRebuildTime = time.second;
        }

        // result.append("Min stage rebuild data transmission time is " + to_string(n*MaxRebuildTime)+"\n");

        auto targetStageObj = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution();
        //get the hashtable build time
        double stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildTimeofTasks();

        preTime = remainingTime - stageTaskMaxBuildTime;
        stageTaskMaxBuildTime = stageTaskMaxBuildTime/1000;

        // result.append("Max stage task hash build time is " + to_string(stageTaskMaxBuildTime)+"\n");

        //first let we figure out is there enough cpu resources to create new tasks for this stage
        int maxCpuGet = 0;
        if(!this->isStageCanImprove(queryId,stageId,n,maxCpuGet)) {
            reasons.append("#Lack of cpu resources to create tasks!");
        }

        vector<shared_ptr<CpuBottleneckResult>> cpu;
        vector<shared_ptr<NetBottleneckResult>> nettrans;
        vector<shared_ptr<NetBottleneckResult>> netrec;
        findBottlenecksForStageExtern(queryId,stageId,cpu,nettrans,netrec);

        //then we figure out is there net rec bottleneck on this stage
        for(auto rec : netrec)
        {
            if(rec->getStageId() == stageId)
            {
                //preTime = remainingTime;
                reasons.append("#NIC receive bottleneck!");
            }
        }

        //then we figure out if the left upstream stage's net and cpu usage can be boosted by n

        auto childStages = targetStage->getStageExecutionAndSchedulerByStagId(stageId)->getStageLinkage();
        if(!childStages->getChildStages().empty()) {
            auto leftChild = childStages->getChildStages()[0];

            if(!leftChild->isBufferNeedPartitioning())
            {
                stageTaskMaxBuildTime = targetStageObj->getMaxHashTableBuildComputingTimeofTasks()/1000+MaxRebuildTime;
            }

            findBottlenecksForStageExtern(queryId,leftChild->getStageId().getId(),cpu,nettrans,netrec);

            auto netIm = this->query_stage_node_net_improvement[queryId];
            auto shuffleIm = this->query_stage_node_shuffle_improvement[queryId];
            auto cpuIm = this->query_stage_node_cpu_improvement[queryId];

            double netImprove = 0;
            double shuffleImprove = 0;
            double cpuImprove = 0;
            double cpuImproveByThreads = 0;
            if(netIm != NULL && shuffleIm != NULL && cpuIm.first != NULL) {
                netImprove = netIm->getStageMinImprovement(leftChild->getStageId().getId());
                shuffleImprove = shuffleIm->getStageMinImprovement(leftChild->getStageId().getId());
                cpuImprove = cpuIm.first->getStageMinImprovement(leftChild->getStageId().getId());
                cpuImproveByThreads = cpuIm.second->getStageAVGImprovement(leftChild->getStageId().getId());
            }

            for(auto trans : nettrans)
            {
                if(trans->getStageId() == leftChild->getStageId().getId())
                {
                    // reasons.append("upstream net trans bottleneck!");
                }
            }

            if (netImprove + 1 > n)
                ;
            else {
                // reasons.append("#Upstream NIC transmission bottleneck! Max improvement:" +
                //             to_string(netImprove + 1));

                reasons.append(" #Upstream NIC transmission bottleneck!");
            }
            if (cpuImprove + 1 > n)
                ;
            else {
                // reasons.append("#Upstream lack of cpu resources! Max improvement:" +
                //             to_string(cpuImprove + 1));

                reasons.append(" #Upstream lack of cpu resources!");
            }
            if (cpuImproveByThreads + 1 > n)
                ;
            else {
                // reasons.append("#Upstream lack of cpu resources! Max improvement:" +
                //             to_string(cpuImprove + 1));

                reasons.append(" #Upstream DOP is low!");
            }
            if (shuffleImprove + 1 > n)
                ;
            else {
                if(shuffleImprove + 1 > 1) {

                    reasons.append(" #Upstream lack of cpu resources for shuffle");
                    //reasons.append("#Upstream lack of cpu resources for shuffle, Max improvement:" +
                    //             to_string(shuffleImprove + 1));
                }
            }




            double minFactor = 9999999.99;
            if(netImprove + 1 < minFactor && netImprove + 1 > 1) {
                minFactor = netImprove + 1;
                computingMethods.append("netImproveFactor="+ to_string(netImprove+1)+";");
            }


            if(leftChild->isHavingAdaptiveDOP()){
                if(cpuImprove + 1 < minFactor && cpuImprove + 1 > 1) {
                    minFactor = cpuImprove + 1;
                    computingMethods.append("cpuImproveFactor="+ to_string(cpuImprove+1)+";");
                }
            }
            else
            {
                double minCPUFactor = 9999999999;
                if(cpuImprove + 1 > 1 && cpuImprove + 1 < minCPUFactor) {
                    minCPUFactor = cpuImprove + 1;
                    computingMethods.append("cpuImproveFactor="+ to_string(cpuImprove+1)+";");
                }
                if(cpuImproveByThreads + 1 > 1 && cpuImproveByThreads + 1 < minCPUFactor) {
                    minCPUFactor = cpuImproveByThreads + 1;
                    computingMethods.append("cpuByThreadNumsImproveFactor="+ to_string(cpuImproveByThreads+1)+";");
                }
                if(minCPUFactor < minFactor)
                    minFactor = minCPUFactor;
            }

            if(shuffleImprove + 1 < minFactor && shuffleImprove + 1 > 1) {
                minFactor = shuffleImprove + 1;
                computingMethods.append("shuffleImproveImproveFactor="+ to_string(shuffleImprove+1)+";");
            }
            if(n < minFactor)
                minFactor = n;
            if(minFactor == 9999999)
                minFactor = 1;




            if(minFactor != n)
            {
                prediction.append("Can't accelerate "+ to_string(n)+" times! Max is "+ to_string(minFactor)+" !\n");
            }


            computingMethods.append("remainingTime="+ to_string(remainingTime)+";");
            computingMethods.append("stageTaskMaxBuildTime="+ to_string(stageTaskMaxBuildTime)+";");
            computingMethods.append("minFactor="+ to_string(minFactor)+";");


            preTime = (remainingTime-stageTaskMaxBuildTime)/minFactor;
            preTime += stageTaskMaxBuildTime;

            prediction.append("Predicted time: ["+to_string(preTime)+"] ");


            computingMethods.append("Predicted time: =#"+ to_string(preTime)+"#;");
            if(n*MaxRebuildTime > preTime)
            {
                reasons.append(" !!Warning: Data transmission time is greater than predicted time!");
            }

        }
        else
        {
            preTime = remainingTime;
            reasons.append(" **Need upgrade the hardware configuration.");

        }

        return computingMethods;

    }


    void showAllAnalyze()
    {

        spdlog::info("-------------------------------queryBottleneckResult-------------------------------");
        for(auto re : this->queryBottleneckResult){
            spdlog::info("Query "+re.first);
            for(auto bottleneck : re.second)
            {
                spdlog::info("Stage "+ to_string(bottleneck->getStageId()) + " has bottleneck: "+bottleneck->getBottleneckType());
            }
        }

        spdlog::info("---------------------------cpuBottleneckResult-------------------------------------");
        for(auto re : this->cpuBottleneckResult){
            spdlog::info("Query "+re.first);
            for(auto bottleneck : re.second)
            {
                auto taskIds = bottleneck->getTaskIds();
                string ids;
                for(auto task : taskIds)
                    ids.append(to_string(task)+",");
                if(!ids.empty())
                    ids.pop_back();
                spdlog::info("On node "+bottleneck->getNode()+": Stage "+
                to_string(bottleneck->getStageId()) +" <"+ids+"> has cpu bottlenects, max "+
                to_string(bottleneck->getMaxValue()) + "cur " + to_string(bottleneck->getCurValue()));
            }
        }

        spdlog::info("--------------------------netTransBottleneckResult----------------------------------");
        for(auto re : this->netTransBottleneckResult){
            spdlog::info("Query "+re.first);
            for(auto bottleneck : re.second)
            {
                auto taskIds = bottleneck->getTaskIds();
                string ids;
                for(auto task : taskIds)
                    ids.append(to_string(task)+",");
                if(!ids.empty())
                    ids.pop_back();
                spdlog::info("On node "+bottleneck->getNode()+": Stage "+
                             to_string(bottleneck->getStageId()) +" <"+ids+"> has net trans bottlenects, max "+
                             to_string(bottleneck->getMaxValue()) + "cur " + to_string(bottleneck->getCurValue()));
            }
        }

        spdlog::info("-----------------------------------netRecBottleneckResult----------------------------");
        for(auto re : this->netRecBottleneckResult){
            spdlog::info("Query "+re.first);
            for(auto bottleneck : re.second)
            {
                auto taskIds = bottleneck->getTaskIds();
                string ids;
                for(auto task : taskIds)
                    ids.append(to_string(task)+",");
                if(!ids.empty())
                    ids.pop_back();

                spdlog::info("On node "+bottleneck->getNode()+": Stage "+
                             to_string(bottleneck->getStageId())+" <"+ids+"> has net rec bottlenects, max "+
                             to_string(bottleneck->getMaxValue()) + "cur " + to_string(bottleneck->getCurValue()));
            }
        }



        spdlog::info("--------------------------------query_stage_node_net_improvement---------------------");
        for(auto re : this->query_stage_node_net_improvement){
            spdlog::info("Query "+re.first);
            re.second->view();
        }

        spdlog::info("----------------------query_stage_node_shuffle_improvement---------------------------");
        for(auto re : this->query_stage_node_shuffle_improvement){
            spdlog::info("Query "+re.first);
            re.second->view();
        }

        spdlog::info("----------------------query_stage_node_cpu_improvement---------------------------");
        for(auto re : this->query_stage_node_cpu_improvement){
            spdlog::info("Query "+re.first);
            spdlog::info("By node:");
            re.second.first->view();
            spdlog::info("By threads:");
            re.second.second->view();
        }

        spdlog::info("------------------------stageTaskMaxImproveRatioResult-------------------------------");
        for(auto re : this->stageTaskMaxImproveRatioResult){
            spdlog::info("Query "+re.first);
            for(auto stage : re.second) {
                spdlog::info("Stage Id:" + to_string(stage->getStageId()) + " maximum number of tasks on cur cluster:" +
                             to_string(stage->getMaxRatio()));
            }
        }

    }












};


#endif //OLVP_PPM_HPP
