//
// Created by zxk on 11/18/23.
//

#ifndef OLVP_INSTRUCTIONEXECUTOR_HPP
#define OLVP_INSTRUCTIONEXECUTOR_HPP



#include "Instruction.hpp"
#include "../Query/QueryManager.hpp"
#include "ScriptExecutionContext.hpp"
#include "ScriptQueryMonitor.hpp"
#include "../Execution/Scheduler/QueryInfos/StageProcessingTimeDict.hpp"
using namespace std;


class InstructionExecutor
{

    typedef bool  (InstructionExecutor::*Fun_ptr)(Instruction instruction,ScriptExecutionContext &context);
    map<string, Fun_ptr> funcMap;
    shared_ptr<QueryManager> queryManager;
    shared_ptr<ScriptQueryMonitor> monitor;
    shared_ptr<StageProcessingTimeDict> stageProcessingTimeDict;

    bool recordProcessingtime = false;

public:
    InstructionExecutor(shared_ptr<QueryManager> queryManager,shared_ptr<ScriptQueryMonitor> monitor)
    {
        this->queryManager = queryManager;
        this->monitor = monitor;

        initInstructions();
    }
    void initInstructions()
    {
        funcMap.insert(make_pair("WAIT_QUERY", &InstructionExecutor::WAIT_QUERY));
        funcMap.insert(make_pair("START_QUERY", &InstructionExecutor::START_QUERY));
        funcMap.insert(make_pair("BEGIN", &InstructionExecutor::BEGIN));
        funcMap.insert(make_pair("END", &InstructionExecutor::END));
        funcMap.insert(make_pair("ADD_CONCURRENCY", &InstructionExecutor::ADD_CONCURRENCY));
        funcMap.insert(make_pair("ADD_PARALLELISM", &InstructionExecutor::ADD_PARALLELISM));
        funcMap.insert(make_pair("ADD_PARALLELISM_TO_INIT", &InstructionExecutor::ADD_PARALLELISM_TO_INIT));
        funcMap.insert(make_pair("R_CONFIG", &InstructionExecutor::RUNTIME_CONFIG));
        funcMap.insert(make_pair("LISTEN", &InstructionExecutor::LISTEN));
        funcMap.insert(make_pair("ECHO", &InstructionExecutor::ECHO_INFO));
        funcMap.insert(make_pair("PREDICT_TIME", &InstructionExecutor::PREDICT_TIME));

        funcMap.insert(make_pair("START_AND_COLLECT", &InstructionExecutor::START_AND_COLLECT));
    }


    bool LISTEN(Instruction instruction,ScriptExecutionContext &context)
    {

        spdlog::debug("Listen the event of stage build Hash Table!");
        return true;
    }
    bool RUNTIME_CONFIG(Instruction instruction,ScriptExecutionContext &context)
    {
        vector<string> parameters = instruction.getParameters();

        if(parameters[0]=="SET_TABLE_SCAN_SIZE")
        {
            string queryId;
            if(context.getQueryId(queryId))
            {
                spdlog::error("Config info should be provided before the query execution.");
                return true;
            }
            vector<string> paras;
            for(int i = 1 ; i < parameters.size() ; i++)
                paras.push_back(parameters[i]);

            context.addConfig(parameters[0],paras);
            return true;

        }
        return true;

    }



    bool BEGIN(Instruction instruction,ScriptExecutionContext &context)
    {

        return true;
    }
    bool END(Instruction instruction,ScriptExecutionContext &context)
    {
        string queryId;
        if(!context.getQueryId(queryId)){ this->monitor->deleteQuery(queryId);return false;}
        while(true)
        {
            sleep_for(std::chrono::milliseconds(1000));
            if(queryManager->isQueryFinished(queryId) || queryManager->isQueryCanceled(queryId)) break;
        }
        if(queryManager->isQueryCanceled(queryId)) {
            this->monitor->deleteQuery(queryId);
            return true;
        }

        nlohmann::json info;
        info["Type"] = "QUERYTIME";
        info["ExecutionTime"] = this->queryManager->getQueryExecutionTime(queryId);

        map<int,map<int,string>> buildInfos;

        this->queryManager->getStageBuildRecords(queryId,buildInfos);

        nlohmann::json buildRecords = nlohmann::json::array();
        for(auto a:buildInfos)
        {
            for(auto b : a.second)
            {
                nlohmann::json buildRecord;
                spdlog::debug(to_string(a.first) +"-"+ to_string(b.first) +"-"+ b.second);
                buildRecord["stageId"] = a.first;
                buildRecord["taskId"] = b.first;
                buildRecord["buildTimeStamp"] = b.second;
                buildRecords.push_back(buildRecord);
            }
        }
        info["buildRecords"] = buildRecords;


        if(this->recordProcessingtime)
        {
            this->recordProcessingtime = false;
            string queryName;
            context.getQueryName(queryName);
            auto stageProcTimes = queryManager->getQueryStageProcessingTimes(queryId);
            auto stageDOPs = queryManager->getQueryStageDOPs(queryId);

            int maxDOP = 0;
            for(auto stage : stageDOPs)
                if(stage.second > maxDOP)
                    maxDOP = stage.second;


            for(auto pt : stageProcTimes)
                this->stageProcessingTimeDict->addNewInfo(queryName,pt.first,maxDOP,pt.second);
        }
        if(this->recordProcessingtime)
            this->stageProcessingTimeDict->save();

        this->monitor->addInfo(queryId,info.dump());

        this->monitor->deleteQuery(queryId);



        return true;
    }


    bool START_AND_COLLECT(Instruction instruction,ScriptExecutionContext &context) {

        vector<string> parameters = instruction.getParameters();

        string queryName = parameters[0];
        string re;
        if(context.getRuntimeConfigs().size() > 0)
            re = queryManager->Give_Me_A_Query(queryName,context.getRuntimeConfigs());
        else
            re = queryManager->Give_Me_A_Query(queryName);

        if(re == "NULL") {
            spdlog::error("Start query failed!");
            return false;
        }
        spdlog::info("Query:"+re+" is started!");

        this->monitor->addQuery(re);

        nlohmann::json info;
        info["Type"] = "QUERYINFO";
        info["QueryId"] = re;

        ExecutionConfig config;
        info["Initial_intra_task_concurrency"] = config.getInitial_intra_task_concurrency();
        info["Initial_hash_partition_concurrency"] = config.getInitial_hash_partition_concurrency();
        info["Initial_intra_stage_concurrency"] = config.getInitial_intra_stage_concurrency();
        info["TEST_system_add_intra_stage_concurrency"] = config.getTEST_system_add_intra_stage_concurrency();
        info["TEST_system_add_intra_task_concurrency"] = config.getTEST_system_add_intra_task_concurrency();

        monitor->addInfo(re,info.dump());

        if(this->stageProcessingTimeDict == NULL)
            this->stageProcessingTimeDict = make_shared<StageProcessingTimeDict>();

        this->recordProcessingtime = true;

        return context.setQueryId(re) && context.setQueryName(queryName);
    }



    bool START_QUERY(Instruction instruction,ScriptExecutionContext &context) {
        vector<string> parameters = instruction.getParameters();

        string re;
        string queryName = parameters[0];
        if(context.getRuntimeConfigs().size() > 0)
            re = queryManager->Give_Me_A_Query(queryName,context.getRuntimeConfigs());
        else
            re = queryManager->Give_Me_A_Query(queryName);

        if(re == "NULL") {
            spdlog::error("Start query failed!");
            return false;
        }
        spdlog::info("Query:"+re+" is started!");

        this->monitor->addQuery(re);

        nlohmann::json info;
        info["Type"] = "QUERYINFO";
        info["QueryId"] = re;

        ExecutionConfig config;
        info["Initial_intra_task_concurrency"] = config.getInitial_intra_task_concurrency();
        info["Initial_hash_partition_concurrency"] = config.getInitial_hash_partition_concurrency();
        info["Initial_intra_stage_concurrency"] = config.getInitial_intra_stage_concurrency();
        info["TEST_system_add_intra_stage_concurrency"] = config.getTEST_system_add_intra_stage_concurrency();
        info["TEST_system_add_intra_task_concurrency"] = config.getTEST_system_add_intra_task_concurrency();

        monitor->addInfo(re,info.dump());

        return context.setQueryId(re) && context.setQueryName(queryName);
    }


    bool WAIT_QUERY(Instruction instruction,ScriptExecutionContext &context) {
        vector<string> parameters = instruction.getParameters();

        int time = atoi(parameters[0].c_str());
        sleep_for(std::chrono::milliseconds(time));
        return true;
    }

    bool ADD_CONCURRENCY(Instruction instruction,ScriptExecutionContext &context)
    {
        vector<string> parameters = instruction.getParameters();
        string queryId;
        if(!context.getQueryId(queryId)) return false;

        if(parameters[0]=="QUERY")
        {

            int degree = atoi(parameters[1].c_str());
            nlohmann::json info;
            info["Type"] = "IQRS";
            info["Operation"] = "ADD_CONCURRENCY-QUERY";
            info["Value"] = to_string(degree);
            info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();
            monitor->addInfo(queryId,info.dump());

            queryManager->addQueryConcurrncy(queryId,degree);

        }
        else if(parameters[0] == "STAGE") {

            int stageId = atoi(parameters[1].c_str());

            int degree = atoi(parameters[2].c_str());
            if (this->queryManager->isStageOfQueryExist(queryId, stageId)) {

                nlohmann::json info;
                info["Type"] = "IQRS";
                info["Operation"] = "ADD_CONCURRENCY-STAGE," + to_string(stageId);
                info["Value"] = to_string(degree);
                info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();
                monitor->addInfo(queryId, info.dump());

                this->queryManager->addStageOfQueryConcurrency(queryId,stageId,degree);
            }
        }
        return true;
    }

    bool ECHO_INFO(Instruction instruction,ScriptExecutionContext &context)
    {

        string queryId;
        if(!context.getQueryId(queryId)) return false;
        vector<string> parameters = instruction.getParameters();
        string infoEcho = parameters[0];
        nlohmann::json info;
        info["Type"] = "Echo";
        info["value"] = infoEcho;
        info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();
        monitor->addInfo(queryId,info.dump());

        return true;
    }

    bool ADD_PARALLELISM(Instruction instruction,ScriptExecutionContext &context)
    {
        vector<string> parameters = instruction.getParameters();
        string queryId;
        if(!context.getQueryId(queryId)) return false;
        if(parameters[0]=="QUERY")
        {
            int degree = atoi(parameters[1].c_str());


            nlohmann::json info;
            info["Type"] = "IQRS";
            info["Operation"] = "ADD_PARALLELISM-QUERY";
            info["Value"] = to_string(degree);
            info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();
            monitor->addInfo(queryId,info.dump());
            queryManager->addQueryParallelism(queryId,degree);
        }
        else if(parameters[0] == "STAGE")
        {
            int stageId = atoi(parameters[1].c_str());
            int degree = atoi(parameters[2].c_str());
            if(this->queryManager->isStageOfQueryScalable(queryId,stageId))
            {
                nlohmann::json info;
                info["Type"] = "IQRS";
                info["Operation"] = "ADD_PARALLELISM-STAGE,"+ to_string(stageId);
                info["Value"] = to_string(degree);
                info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();

                monitor->addInfo(queryId,info.dump());

                this->queryManager->addStageTaskGroupConcurrent(queryId,stageId,degree);
            }
            else
            {
                spdlog::error("Stage "+to_string(stageId)+" is not scalable!");
            }
        }
        return true;
    }

    bool ADD_PARALLELISM_TO_INIT(Instruction instruction,ScriptExecutionContext &context)
    {
        vector<string> parameters = instruction.getParameters();
        string queryId;
        if(!context.getQueryId(queryId)) return false;
        if(parameters[0]=="QUERY")
        {
            int degree = atoi(parameters[1].c_str());


            nlohmann::json info;
            info["Type"] = "IQRS";
            info["Operation"] = "ADD_PARALLELISM_TO_INIT-QUERY";
            info["Value"] = to_string(degree);
            info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();
            monitor->addInfo(queryId,info.dump());
            queryManager->addQueryParallelismUsingInitialNodes(queryId,degree);
        }
        else if(parameters[0] == "STAGE")
        {
            int stageId = atoi(parameters[1].c_str());
            int degree = atoi(parameters[2].c_str());
            if(this->queryManager->isStageOfQueryScalable(queryId,stageId))
            {
                nlohmann::json info;
                info["Type"] = "IQRS";
                info["Operation"] = "ADD_PARALLELISM_TO_INIT-STAGE,"+ to_string(stageId);
                info["Value"] = to_string(degree);
                info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();

                monitor->addInfo(queryId,info.dump());

                this->queryManager->addStageTaskGroupConcurrentToInit(queryId,stageId,degree);
            }
            else
            {
                spdlog::error("Stage "+to_string(stageId)+" is not scalable!");
            }
        }
        return true;
    }


    bool PREDICT_TIME(Instruction instruction,ScriptExecutionContext &context)
    {
        vector<string> parameters = instruction.getParameters();
        string queryId;

        int stageId = atoi(parameters[1].c_str());
        double degree = atof(parameters[2].c_str());

        if(!context.getQueryId(queryId)) return false;

        string preTime = this->queryManager->getPPM()->getStagePredictedTime(queryId,stageId,degree);


        nlohmann::json info;
        info["Type"] = "PREDICTTIME";
        info["value"] = preTime;
        info["TimeStamp"] = TimeCommon::getCurrentTimeStamp();
        monitor->addInfo(queryId,info.dump());
        return true;
    }

    bool executeInstruction(Instruction ins,ScriptExecutionContext &context)
    {
        if(this->funcMap.count(ins.getInstruction()) == 0)
        {
            spdlog::error("Execution ERROR! Unknown Instruction "+ins.getInstruction()+"!");
            return false;
        }
        return (this->*funcMap[ins.getInstruction()])(ins,context);
    }

};

#endif //OLVP_INSTRUCTIONEXECUTOR_HPP
