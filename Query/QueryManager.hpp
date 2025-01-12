//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_QUERYMANAGER_HPP
#define OLVP_QUERYMANAGER_HPP

#include "../Execution/Scheduler/SqlQueryExecution.hpp"
#include "../TpchTest/Querys/Query1.hpp"
#include "../TpchTest/Querys/Query1_NL.hpp"
#include "../TpchTest/Querys/Query1_single.hpp"
#include "../TpchTest/Querys/Query1_ssingle.hpp"
#include "../TpchTest/Querys/Query2.hpp"
#include "../TpchTest/Querys/Query2_NL.hpp"
#include "../TpchTest/Querys/Query2_hash.hpp"
#include "../TpchTest/Querys/Query2_hash_NL.hpp"
#include "../TpchTest/Querys/Query2_single.hpp"

#include "../TpchTest/Querys/Query3.hpp"
#include "../TpchTest/Querys/Query3_NL.hpp"
#include "../TpchTest/Querys/Query3_hash.hpp"
#include "../TpchTest/Querys/Query3_hash_NL.hpp"
#include "../TpchTest/Querys/Query3_single.hpp"
#include "../TpchTest/Querys/Query4.hpp"
#include "../TpchTest/Querys/Query4_NL.hpp"
#include "../TpchTest/Querys/Query4_NL_MulHashBuild.hpp"
#include "../TpchTest/Querys/Query4_hash.hpp"
#include "../TpchTest/Querys/Query4_hash_NL.hpp"
#include "../TpchTest/Querys/Query4_single.hpp"
#include "../TpchTest/Querys/Query5.hpp"
#include "../TpchTest/Querys/Query5_NL.hpp"
#include "../TpchTest/Querys/Query5_hash.hpp"
#include "../TpchTest/Querys/Query5_hash_NL.hpp"
#include "../TpchTest/Querys/Query5_single.hpp"
#include "../TpchTest/Querys/Query6_NL.hpp"
#include "../TpchTest/Querys/Query6.hpp"
#include "../TpchTest/Querys/Query6_single.hpp"
#include "../TpchTest/Querys/Query7.hpp"
#include "../TpchTest/Querys/Query7_NL.hpp"
#include "../TpchTest/Querys/Query7_hash.hpp"
#include "../TpchTest/Querys/Query7_hash_NL.hpp"
#include "../TpchTest/Querys/Query7_single.hpp"
#include "../TpchTest/Querys/Query8.hpp"
#include "../TpchTest/Querys/Query8_NL.hpp"
#include "../TpchTest/Querys/Query8_hash.hpp"
#include "../TpchTest/Querys/Query8_hash_NL.hpp"
#include "../TpchTest/Querys/Query8_single.hpp"
#include "../TpchTest/Querys/Query9.hpp"
#include "../TpchTest/Querys/Query9_NL.hpp"
#include "../TpchTest/Querys/Query9_hash.hpp"
#include "../TpchTest/Querys/Query9_hash_NL.hpp"
#include "../TpchTest/Querys/Query9_hashtest.hpp"
#include "../TpchTest/Querys/Query9_single.hpp"
#include "../TpchTest/Querys/Query10.hpp"
#include "../TpchTest/Querys/Query10_NL.hpp"
#include "../TpchTest/Querys/Query10_2.hpp"
#include "../TpchTest/Querys/Query10_hash.hpp"
#include "../TpchTest/Querys/Query10_hash_NL.hpp"
#include "../TpchTest/Querys/Query10_single.hpp"
#include "../TpchTest/Querys/Query11.hpp"
#include "../TpchTest/Querys/Query11_NL.hpp"
#include "../TpchTest/Querys/Query11_hash.hpp"
#include "../TpchTest/Querys/Query11_hash_NL.hpp"
#include "../TpchTest/Querys/Query11_single.hpp"
#include "../TpchTest/Querys/Query12.hpp"
#include "../TpchTest/Querys/Query12_NL.hpp"
#include "../TpchTest/Querys/Query12_hash.hpp"
#include "../TpchTest/Querys/Query12_hash_NL.hpp"
#include "../TpchTest/Querys/Query12_single.hpp"

#include "../TpchTest/Querys/Query2_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query3_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query4_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query5_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query7_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query8_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query10_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query11_ShuffleStage.hpp"
#include "../TpchTest/Querys/Query12_ShuffleStage.hpp"

#include "../TpchTest/Querys/Query2Join.hpp"
#include "../TpchTest/Querys/Query2Join_NL.hpp"
#include "../TpchTest/Querys/Query2Join_NL_SmallOrders.hpp"
#include "../TpchTest/Querys/Query2Join_NL_SmallOrders_ProbeShuffle.hpp"
#include "../TpchTest/Querys/Query2Join_OC.hpp"
#include "../TpchTest/Querys/Query2Join_OC_ProbeShuffle.hpp"

#include "../TpchTest/Querys/Query2Join_single.hpp"
#include "../TpchTest/Querys/Query2SJoin.hpp"
#include "../TpchTest/Querys/Query2JoinShuffleStage.hpp"
#include "../TpchTest/Querys/Query2JoinMulHashBuild.hpp"

#include "../TpchTest/Querys/Query2JoinTwoShuffleStage.hpp"
#include "../TpchTest/Querys/Query2Join2TwoShuffleStage.hpp"
#include "../TpchTest/Querys/Query2Join2ShuffleStage.hpp"


#include "../TpchTest/Querys/SimpleTest/Query_SupplierJoinSupplier.hpp"

#include "../Tuning/Prediction/PPM.hpp"
#include "../Tuning/Prediction/ClusterContext.hpp"

#include "../Tuning/AutoTunerManager.hpp"

#include "../Utils/TimeCommon.hpp"
class QueryManager
{

    PlanNode *tree;
    shared_ptr<map<string,shared_ptr<SqlQueryExecution>>> querys;

    map<string,shared_ptr<RegQuery>> regQueryList;
    NodesManager nodesManager;

    shared_ptr<PPM> ppm = NULL;


    shared_ptr<AutoTunerManager> autoTunerManager;
public:

    QueryManager(){
        spdlog::info("QueryServer Start!");
        regQuerys();
        this->querys = make_shared<map<string,shared_ptr<SqlQueryExecution>>>();
        this->autoTunerManager = make_shared<AutoTunerManager>();
    }


    void regQuerys()
    {
        regQueryList["Q1L"] = make_shared<Query1>();
        regQueryList["Q1"] = make_shared<Query1_NL>();
        regQueryList["Q1S"] = make_shared<Query1_single>();
        regQueryList["Q1SS"] = make_shared<Query1_ssingle>();

        regQueryList["Q2L"] = make_shared<Query2>();
        regQueryList["Q2"] = make_shared<Query2_NL>();
        regQueryList["Q2_h"] = make_shared<Query2_hash>();
        regQueryList["Q2_h_NL"] = make_shared<Query2_hash_NL>();
        regQueryList["Q2_ss"] = make_shared<Query2_ShuffleStage>();
        regQueryList["Q2S"] = make_shared<Query2_single>();


        regQueryList["Q3L"] = make_shared<Query3>();
        regQueryList["Q3"] = make_shared<Query3_NL>();
        regQueryList["Q3_h"] = make_shared<Query3_hash>();
        regQueryList["Q3_h_NL"] = make_shared<Query3_hash_NL>();
        regQueryList["Q3_ss"] = make_shared<Query3_ShuffleStage>();
        regQueryList["Q3S"] = make_shared<Query3_single>();

        regQueryList["Q4L"] = make_shared<Query4>();
        regQueryList["Q4"] = make_shared<Query4_NL>();
        regQueryList["Q4NL_HB"] = make_shared<Query4_NL_MulHashBuild>();

        regQueryList["Q4_h"] = make_shared<Query4_hash>();
        regQueryList["Q4_h_NL"] = make_shared<Query4_hash_NL>();
        regQueryList["Q4_ss"] = make_shared<Query4_ShuffleStage>();
        regQueryList["Q4S"] = make_shared<Query4_single>();

        regQueryList["Q5L"] = make_shared<Query5>();
        regQueryList["Q5"] = make_shared<Query5_NL>();
        regQueryList["Q5_h"] = make_shared<Query5_hash>();
        regQueryList["Q5_h_NL"] = make_shared<Query5_hash_NL>();
        regQueryList["Q5_ss"] = make_shared<Query5_ShuffleStage>();
        regQueryList["Q5S"] = make_shared<Query5_single>();


        regQueryList["Q6L"] = make_shared<Query6>();
        regQueryList["Q6"] = make_shared<Query6_NL>();
        regQueryList["Q6S"] = make_shared<Query6_single>();

        regQueryList["Q7L"] = make_shared<Query7>();
        regQueryList["Q7"] = make_shared<Query7_NL>();
        regQueryList["Q7_h"] = make_shared<Query7_hash>();
        regQueryList["Q7_h_NL"] = make_shared<Query7_hash_NL>();
        regQueryList["Q7_ss"] = make_shared<Query7_ShuffleStage>();
        regQueryList["Q7S"] = make_shared<Query7_single>();

        regQueryList["Q8L"] = make_shared<Query8>();
        regQueryList["Q8"] = make_shared<Query8_NL>();
        regQueryList["Q8_h"] = make_shared<Query8_hash>();
        regQueryList["Q8_h_NL"] = make_shared<Query8_hash_NL>();
        regQueryList["Q8_ss"] = make_shared<Query8_ShuffleStage>();
        regQueryList["Q8S"] = make_shared<Query8_single>();

        regQueryList["Q9L"] = make_shared<Query9>();
        regQueryList["Q9"] = make_shared<Query9_NL>();
        regQueryList["Q9S"] = make_shared<Query9_single>();

        regQueryList["Q10L"] = make_shared<Query10>();
        regQueryList["Q10"] = make_shared<Query10_NL>();
        regQueryList["Q102"] = make_shared<Query10_2>();
        regQueryList["Q10_h"] = make_shared<Query10_hash>();
        regQueryList["Q10_h_NL"] = make_shared<Query10_hash_NL>();
        regQueryList["Q10_ss"] = make_shared<Query10_ShuffleStage>();
        regQueryList["Q10S"] = make_shared<Query10_single>();

        regQueryList["Q11L"] = make_shared<Query11>();
        regQueryList["Q11"] = make_shared<Query11_NL>();
        regQueryList["Q11_h"] = make_shared<Query11_hash>();
        regQueryList["Q11_h_NL"] = make_shared<Query11_hash_NL>();
        regQueryList["Q11_ss"] = make_shared<Query11_ShuffleStage>();
        regQueryList["Q11S"] = make_shared<Query11_single>();

        regQueryList["Q12L"] = make_shared<Query12>();
        regQueryList["Q12"] = make_shared<Query12_NL>();
        regQueryList["Q12_h"] = make_shared<Query12_hash>();
        regQueryList["Q12_h_NL"] = make_shared<Query12_hash_NL>();
        regQueryList["Q12_ss"] = make_shared<Query12_ShuffleStage>();
        regQueryList["Q12S"] = make_shared<Query12_single>();

        regQueryList["Q2_J"] = make_shared<Query2_Join>();
        regQueryList["Q2_J_NL_HB"] = make_shared<Query2JoinMulHashBuild>();

        regQueryList["Q2_JS"] = make_shared<Query2_Join_single>();
        regQueryList["Q2_J_SS"] = make_shared<Query2_Join_ShuffleStage>();
        regQueryList["Q2_J_2SS"] = make_shared<Query2_Join_TwoShuffleStage>();
        regQueryList["Q2_J2_2SS"] = make_shared<Query2_Join2_TwoShuffleStage>();
        regQueryList["Q2_J2_SS"] = make_shared<Query2_Join2_ShuffleStage>();
        regQueryList["Q2_J_NL"] = make_shared<Query2_Join_NL>();

        regQueryList["Q2_J_NL_SO"] = make_shared<Query2_Join_NL_SmallOrders>();
        regQueryList["Q2_J_NL_SOPS"] = make_shared<Query2_Join_NL_SmallOrders_ProbeShuffle>();

        regQueryList["Q2_J_OC_SC"] = make_shared<Query2_Join_OC>();
        regQueryList["Q2_J_OC_SCPS"] = make_shared<Query2_Join_OC_ProbeShuffle>();


        regQueryList["Q2_SJ"] = make_shared<Query2_SJoin>();

        regQueryList["Q_SupplierJoinSupplier"] = make_shared<Query_SupplierJoinSupplier>();


  //      regQueryList["test1"] = this->getATestTree();
  //      regQueryList["test2"] = this->getATestTree2();

    }
    string getQuerySql(string queryName)
    {
        if(this->regQueryList.count(queryName) > 0)
            return this->regQueryList[queryName]->getSql();
        else
            return "Cannot identify this query name.";
    }

    string listAllRegQueries()
    {
        string all;
        for(auto query : this->regQueryList)
        {
            all+=(query.first+'\n');
        }
        return all;
    }



    string Give_Me_A_Query(string id)
    {

        if(this->regQueryList.count(id) != 0)
            this->tree = this->regQueryList[id]->getPlanTree();
        else
            return "NULL";

        string queryId = TimeCommon::getCurrentTimeString()+"TPCH-QUERY-"+id+"-"+UUID::create_uuid();
        shared_ptr<Session> session = make_shared<Session>(queryId,-1);
       // session->setRumtimeConfigs({{"SET_TABLE_SCAN_SIZE",{"tpch_test_tpch_1_lineitem","123"}}});
        shared_ptr<SqlQueryExecution> queryExecution = make_shared<SqlQueryExecution>(session,this->tree);
        startQuery(queryId,queryExecution);
        return queryId;
    }

    string ExecuteQuery(string id,PlanNode *root)
    {
        string queryId = TimeCommon::getCurrentTimeString()+"TPCH-QUERY-"+id+"-"+UUID::create_uuid();
        shared_ptr<Session> session = make_shared<Session>(queryId,-1);

        shared_ptr<SqlQueryExecution> queryExecution = make_shared<SqlQueryExecution>(session,root);
        startQuery(queryId,queryExecution);
        return queryId;
    }

    string Give_Me_A_Query(string id,multimap<string,vector<string>> rconfigs)
    {

        if(this->regQueryList.count(id) != 0)
            this->tree = this->regQueryList[id]->getPlanTree();
        else
            return "NULL";

        string queryId = TimeCommon::getCurrentTimeString()+"TPCH-QUERY-"+id+"-"+UUID::create_uuid();
        shared_ptr<Session> session = make_shared<Session>(queryId,-1);
        // session->setRumtimeConfigs({{"SET_TABLE_SCAN_SIZE",{"tpch_test_tpch_1_lineitem","123"}}});
        session->setRumtimeConfigs(rconfigs);
        shared_ptr<SqlQueryExecution> queryExecution = make_shared<SqlQueryExecution>(session,this->tree);
        if(!startQuery(queryId,queryExecution))
            return "NULL";
        return queryId;
    }


    void showAllQueryCpuUsage()
    {
        list<shared_ptr<SqlQueryExecution>> activeQuerys;

        for(auto query : *this->querys)
        {

            if(!query.second->isQueryFinished())
                activeQuerys.push_back(query.second);

        }


        spdlog::info("------------------------------------------------------");
        if(activeQuerys.empty())
        {
            spdlog::info("Query CPU usage: No query is running now.");
        }
        for(auto activeQuery : activeQuerys)
        {
            spdlog::info("QueryID:"+activeQuery->getSession()->getQueryId());
            auto stageCpuUsage = activeQuery->getStagesCpuUsages();

            for(auto stageCpu : stageCpuUsage)
            {
                spdlog::info("\tstageId:"+to_string(stageCpu.first));
                for(auto taskCpu : stageCpu.second){
                    spdlog::info("\t\ttaskId:"+ to_string(taskCpu.taskId) + " total->" + to_string(taskCpu.totalUsage)+
                    " driver->" + to_string(taskCpu.driverUsage)+ " shuffle->" + to_string(taskCpu.shuffleUsage)
                    + " outputRate:" + to_string(taskCpu.throughputBytes)+" |tup:"+ to_string(taskCpu.tb_tup) + " |tdown:"+ to_string(taskCpu.tb_tdown)
                    +" |eup:"+ to_string(taskCpu.eb_tup) + " |edown"+ to_string(taskCpu.eb_tdown));

                }
            }

        }
        spdlog::info("------------------------------------------------------");

    }


    bool startQuery(string queryId,shared_ptr<SqlQueryExecution> queryExecution)
    {
        if(!ClusterServer::getNodesManager()->hasNodes()) {
            spdlog::error("No nodes to be scheduled!");
            return false;
        }
        (*this->querys)[queryId] = queryExecution;
        thread(start,queryExecution).detach();
        return true;
    }

    static void start(shared_ptr<SqlQueryExecution> execution)
    {
        execution->start();
    }

    void viewTaskInfoGroupByNodes()
    {
        ClusterContext clusterContext(this->querys);
        clusterContext.gatherInfos();
        clusterContext.viewTasksGroupByNode();

    }

    void detectBottleneckForQuerys()
    {
        if(this->ppm == NULL)
            this->ppm = make_shared<PPM>(this->querys,ClusterServer::getNodesManager());

        this->ppm->collect();

    }
    string getStagePredictionInfos(string queryId,int stageId,int DOP)
    {
        string result = getPPM()->getStageImprovementPredictionExtern(queryId,stageId,DOP);

        return result;
    }

    string autoTuneByTimeConstraint(string queryId,string timeConstraintBySeconds)
    {
        return this->autoTunerManager->autoTuneByTimeConstraint(queryId,this->getPPM(),timeConstraintBySeconds);
    }

    shared_ptr<AutoTunerManager> getAutoTuneManager()
    {
        return this->autoTunerManager;
    }

    void autoTuneForScriptExecutor(string queryId,string timeConstraintBySeconds)
    {
        this->autoTunerManager->autoTuneByTimeConstraint(queryId,this->getPPM(),timeConstraintBySeconds);
    }

    string getQueryBottlenecks(string queryId)
    {
        string result = getPPM()->getQueryBottleneckStagesExtern(queryId);

        //this->parallelismAutoTuner = make_shared<ParallelismAutoTuner>(this->ppm);
        //this->parallelismAutoTuner->tune(queryId);
       // if(!this->autoTunerWorking) {
      //      this->autoTunerWorking = true;
       //     thread th(autoTuneOnce, this, queryId, this->ppm);
       //     th.detach();
       // }
        return  result;
    }

    string getQueryBottleneckStagesAndAnalyzeExtern(string queryId,string factor)
    {
        string result = getPPM()->getQueryBottleneckStagesAndAnalyzeExtern(queryId,atoi(factor.c_str()));
        return  result;
    }

    shared_ptr<PPM> getPPM()
    {
        if(this->ppm == NULL) {
            this->ppm = make_shared<PPM>(this->querys, ClusterServer::getNodesManager());
        }
        return this->ppm;
    }

    bool isQueryStart(string queryId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = NULL;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];

            bool re = queryExecution->isQueryStart();
            return re;
        }
        else
            return true;
    }


    bool isQueryFinished(string queryId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = NULL;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];

            bool re = queryExecution->isQueryFinished();
            return re;
        }
        else
            return true;
    }
    bool isQueryCanceled(string queryId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = NULL;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];

            bool re = queryExecution->isQueryCanceled();
            return re;
        }
        else
            return true;
    }
    string getQueryExecutionTime(const string& queryId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];

            string re = queryExecution->getQueryExecutionTime();
            return re;
        }
        else
            return "-1";
    }

    map<int,pair<long,long>> getQueryStageProcessingTimes(const string& queryId)
    {
        map<int,pair<long,long>> re;
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];

            re = queryExecution->getStageProcessingTimes();
            return re;
        }
        else
            return re;
    }

    map<int,int> getQueryStageDOPs(const string& queryId)
    {
        map<int,int> re;
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];

            re = queryExecution->getStageDOPs();
            return re;
        }
        else
            return re;
    }

    string getQueryInfo(string queryId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];

            nlohmann::json json = queryExecution->getQueryExecutionInfoObj();
            nlohmann::json queryJson;
            queryJson["queryId"] = queryId;
            queryJson["planTree"] = queryExecution->planTreeToJsonObj();
            queryJson["queryContext"] = json;

            if(queryExecution->isQueryFinished())
            {
                queryJson["originTime"] = queryExecution->getScheduler()->getOriginExecutionTime();
                queryJson["actualTime"] = queryExecution->getScheduler()->getActualExecutionTime();
                queryJson["exeTime"] = queryExecution->getScheduler()->getExecutionTime();
            }


            return queryJson.dump();
        }
        else
            return "NULL";

    }



    string getAllRunningQueryInfo() {

        nlohmann::json queryInfos;
        for(auto query : (*this->querys))
        {
            if(query.second->getState()->getState() == QueryStateMachine::RUNNING) {

                string re = query.second->getQueryExecutionInfo();
                nlohmann::json json = nlohmann::json::parse(re);
                nlohmann::json queryJson;
                queryJson["queryId"] = query.first;
                queryJson["queryContext"] = json;



                queryInfos.push_back(queryJson);

            }
        }
        return queryInfos.dump();

    }
    string getAllFinishedQueryInfo() {

        nlohmann::json queryInfos;
        for(auto query : (*this->querys))
        {
            if(query.second->getState()->isFinished()) {

                string re = query.second->getQueryExecutionInfo();
                nlohmann::json json = nlohmann::json::parse(re);
                nlohmann::json queryJson;
                queryJson["queryId"] = query.first;
                queryJson["queryContext"] = json;

                if(query.second->isQueryFinished())
                {
                    queryJson["originTime"] = query.second->getScheduler()->getOriginExecutionTime();
                    queryJson["actualTime"] = query.second->getScheduler()->getActualExecutionTime();
                    queryJson["exeTime"] = query.second->getScheduler()->getExecutionTime();
                }

                queryInfos.push_back(queryJson);

            }
        }
        return queryInfos.dump();

    }

    string getAllQueryInfo() {

        nlohmann::json queryInfos;
        nlohmann::json queryInfosReverse;
        for(auto query : (*this->querys)) {

            string re = query.second->getQueryExecutionInfo();
            nlohmann::json json = nlohmann::json::parse(re);
            nlohmann::json queryJson;
            queryJson["queryId"] = query.first;
            queryJson["queryContext"] = json;

            if(query.second->isQueryFinished())
            {
                queryJson["originTime"] = query.second->getScheduler()->getOriginExecutionTime();
                queryJson["actualTime"] = query.second->getScheduler()->getActualExecutionTime();
                queryJson["exeTime"] = query.second->getScheduler()->getExecutionTime();
            }

            queryInfos.push_back(queryJson);

        }
       for(int i = queryInfos.size() -1 ; i >= 0 ; i--)
       {
           queryInfosReverse.push_back(queryInfos[i]);
       }

        return queryInfos.dump();

    }


    string getQueryResult(string queryId) {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if ((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->getQueryResult();
        } else
            return "NULL";
    }

    bool isStageOfQueryScalable(string queryId,int stageId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->isStageScalable(stageId);
        }
        else
            return false;
    }

    bool isStageOfQueryTuningKnob(string queryId,int stageId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->isStageisTuningKnob(stageId);
        }
        else
            return false;
    }
    bool getStageBuildRecords(string queryId,map<int,map<int,string>> &result)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        map<int,map<int,string>> null;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            result = queryExecution->getQueryBuildRecords();
            return true;
        }
        else
            return false;
    }
    bool isStageOfQueryExist(string queryId,int stageId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->isStageExist(stageId);
        }
        else
            return false;
    }

    string addStageTaskGroupConcurrent(string queryId,int stageId,int taskNum)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addStageTaskGroupConcurrent(stageId,taskNum);
        }
        else
            return "NULL";
    }

    string addStageTaskGroupConcurrentToInit(string queryId,int stageId,int taskNum)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addStageTaskGroupConcurrentToInit(stageId,taskNum);
        }
        else
            return "NULL";
    }



    string addQueryConcurrncy(string queryId,int degree)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addQueryConcurrency(degree);
        }
        else
            return "NULL";
    }
    string addStageOfQueryConcurrency(string queryId,int stageId,int degree)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addStageConcurrency(stageId,degree);
        }
        else
            return "NULL";
    }

    string addQueryParallelism(string queryId,int degree)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addQueryParallelism(degree);
        }
        else
            return "NULL";
    }

    string addQueryParallelismUsingInitialNodes(string queryId,int degree)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) !=(*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addQueryParallelismUsingInitialNodes(degree);
        }
        else
            return "NULL";
    }

    string addStageConcurrent(string queryId,int stageId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;

        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addStageConcurrent(stageId);
        }
        else
            return "NULL";
    }
    string addStageAllTaskIntraPipelineConcurrent(string queryId,string stageId,string pipelineId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_addStageAllTaskIntraPipelineConcurrent(stageId,pipelineId);
        }
        else
            return "NULL";
    }

    string subStageAllTaskIntraPipelineConcurrent(string queryId,string stageId,string pipelineId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_subStageAllTaskIntraPipelineConcurrent(stageId,pipelineId);
        }
        else
            return "NULL";
    }

    string decreaseStageParallelism(string queryId,int stageId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->Dynamic_decreaseStageParallelism(stageId,1);
        }
        else
            return "NULL";
    }

    string getQueryThroughputs(string queryId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->getQueryThroughputsSnapShot();
        }
        else
            return "NULL";
    }

    string getQueryThroughputsInfo(const string& queryId)
    {
        shared_ptr<SqlQueryExecution> queryExecution = nullptr;
        if((*this->querys).find(queryId) != (*this->querys).end()) {
            queryExecution = (*this->querys)[queryId];
            return queryExecution->getQueryThroughputsInfoSnapShot();
        }
        else
            return "NULL";
    }








};
#endif //OLVP_QUERYMANAGER_HPP
