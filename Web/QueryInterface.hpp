//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_QUERYINTERFACE_HPP
#define OLVP_QUERYINTERFACE_HPP



#include "../System/QueryServer.hpp"
#include <string>
using namespace std;

class QueryInterFace
{

public:
    QueryInterFace(){
    }

    static string getQueryInfo(string queryId)
    {
        return QueryServer::queryServer->getQueryInfo(queryId);
    }
    static string getQueryResult(string queryId)
    {
        return QueryServer::queryServer->getQueryResult(queryId);
    }
    static string getAllRunningQueryInfo() {
        return QueryServer::queryServer->getAllRunningQueryInfo();
    }
    static string getAllQueryInfo() {

        return QueryServer::queryServer->getAllQueryInfo();

    }
    static string getAllRegQueries()
    {
        return QueryServer::queryServer->listAllRegQueries();
    }

    static void give_me_a_query(string id) {
        QueryServer::queryServer->Give_Me_A_Query(id);
    }


    static string addStageTask(string queryId,int stageId)
    {
        return QueryServer::queryServer->addStageConcurrent(queryId,stageId);
    }
    static string addStageTaskPipeline(string queryId,string stageId,string pipelineId)
    {
        return QueryServer::queryServer->addStageAllTaskIntraPipelineConcurrent(queryId,stageId,pipelineId);
    }

    static string subStageTaskPipeline(string queryId,string stageId,string pipelineId)
    {
        return QueryServer::queryServer->subStageAllTaskIntraPipelineConcurrent(queryId,stageId,pipelineId);
    }
    static string decreaseStageParallelism(string queryId,string stageId)
    {
        return QueryServer::queryServer->decreaseStageParallelism(queryId,atoi(stageId.c_str()));
    }


    static string addStageTaskGroup(string queryId,int stageId,int taskNum)
    {
        return QueryServer::queryServer->addStageTaskGroupConcurrent(queryId,stageId,taskNum);
    }

    static string getQueryThroughputs(string queryId)
    {
        return QueryServer::queryServer->getQueryThroughputs(queryId);
    }
    static string getQueryThroughputsInfo(string queryId)
    {
        return QueryServer::queryServer->getQueryThroughputsInfo(queryId);
    }

    static string getQueryBottlenecks(string queryId)
    {
        return QueryServer::queryServer->getQueryBottlenecks(queryId);
    }

    static string autoTuneByTimeConstraint(string queryId,string timeConstraintBySeconds)
    {
        return QueryServer::queryServer->autoTuneByTimeConstraint(queryId,timeConstraintBySeconds);
    }


    static string getQueryBottlenecksAndAnalyze(string queryId,string factor)
    {
        return QueryServer::queryServer->getQueryBottleneckStagesAndAnalyzeExtern(queryId,factor);
    }
    static string getQueryPredictionInfos(string queryId,int stageId,int DOP)
    {
        return QueryServer::queryServer->getStagePredictionInfos(queryId,stageId,DOP);
    }
};



#endif //OLVP_QUERYINTERFACE_HPP
