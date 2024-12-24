//
// Created by zxk on 6/20/23.
//

#ifndef OLVP_EXECUTIONCONFIG_HPP
#define OLVP_EXECUTIONCONFIG_HPP


#include "nlohmann/json.hpp"
#include <string>
#include <iostream>
#include <fstream>
//#include "../Utils/Random.hpp"

using namespace std;

class ExecutionConfig
{
    bool hasRead = false;

    string tableScanBatchSize;
    string initial_intra_task_concurrency;
    string initial_intra_stage_concurrency;
    string log_level;
    string initial_hash_partition_concurrency;
    string initial_task_group_concurrency;
    string TEST_system_add_intra_stage_concurrency;
    string TEST_system_add_intra_task_concurrency;
    string TEST_system_add_task_group_concurrency;
    string initial_shuffle_executor_nums;
    string script_path;
    string task_group_close_mode;
    string doNotUseStorageNode;
    string lazy_task_group_close_mode;
    string intial_plan_used_nodes;
    string intra_task_hash_build_concurrency;
    string tpch_AutoGen_ScaleFactor;
    string autoTuneByPlan;

public:
    ExecutionConfig(){

    }

    bool readConfigFile()
    {
        if(hasRead == true)
            return true;
        string strFileData = "execution.config";
        std::ifstream in;
        in.exceptions(std::ifstream::failbit | std::ifstream::badbit);
        try {
           in.open(strFileData, std::ios::in | std::ios::binary);
        }
        catch (std::system_error& e) {
            std::cerr << e.code().message() << std::endl;
            cout << "Cannot open the execution config file!"<<endl;
            exit(0);
            return false;
        }

        nlohmann::json jsonTree = nlohmann::json::parse(in);
        this->tableScanBatchSize = jsonTree["tableScanBatchSize"];
        this->initial_intra_task_concurrency = jsonTree["initial_intra_task_concurrency"];
        this->initial_intra_stage_concurrency = jsonTree["initial_intra_stage_concurrency"];
        this->initial_hash_partition_concurrency = jsonTree["initial_hash_partition_concurrency"];
        this->intra_task_hash_build_concurrency = jsonTree["intra_task_hash_build_concurrency"];

        this->initial_task_group_concurrency = jsonTree["initial_task_group_concurrency"];
        this->TEST_system_add_intra_task_concurrency = jsonTree["TEST_system_add_intra_task_concurrency"];
        this->TEST_system_add_intra_stage_concurrency = jsonTree["TEST_system_add_intra_stage_concurrency"];
        this->TEST_system_add_task_group_concurrency = jsonTree["TEST_system_add_task_group_concurrency"];
        this->initial_shuffle_executor_nums = jsonTree["initial_shuffle_executor_nums"];
        this->task_group_close_mode = jsonTree["task_group_close_mode"];
        this->doNotUseStorageNode = jsonTree["doNotUseStorageNode"];
        this->lazy_task_group_close_mode = jsonTree["lazy_task_group_close_mode"];

        this->intial_plan_used_nodes = jsonTree["intial_plan_used_nodes"];

        this->log_level = jsonTree["log_level"];
        this->script_path = jsonTree["script_path"];

        if(jsonTree.contains("tpch_AutoGen_ScaleFactor"))
            this->tpch_AutoGen_ScaleFactor = jsonTree["tpch_AutoGen_ScaleFactor"];
        else
            this->tpch_AutoGen_ScaleFactor = "0";

        if(jsonTree.contains("autoTuneByPlan"))
            this->autoTuneByPlan = jsonTree["autoTuneByPlan"];
        else
            this->autoTuneByPlan = "false";

        in.close();
        this->hasRead = true;
        return true;
    }

    string getTableScanBatchSize()
    {
        if(readConfigFile())
            return this->tableScanBatchSize;
        else
            return "4096";
    }

    string getInitial_intra_task_concurrency()
    {
        if(readConfigFile())
            return this->initial_intra_task_concurrency;
        else
            return "1";
    }

    string getInitial_intra_stage_concurrency()
    {
        if(readConfigFile())
            return this->initial_intra_stage_concurrency;
        else
            return "1";
    }
    string getIntra_task_hash_build_concurrency()
    {
        if(readConfigFile())
            return this->intra_task_hash_build_concurrency;
        else
            return "1";
    }
    string getInitial_hash_partition_concurrency()
    {
        if(readConfigFile())
            return this->initial_hash_partition_concurrency;
        else
            return "1";
    }
    string getInitial_task_group_concurrency()
    {
        if(readConfigFile())
            return this->initial_task_group_concurrency;
        else
            return "1";
    }

    string getTEST_system_add_intra_stage_concurrency()
    {
        if(readConfigFile())
            return this->TEST_system_add_intra_stage_concurrency;
        else
            return "-1";
    }
    string getTEST_system_add_intra_task_concurrency()
    {
        if(readConfigFile())
            return this->TEST_system_add_intra_task_concurrency;
        else
            return "-1";
    }

    string getTEST_system_add_task_group_concurrency()
    {
        if(readConfigFile())
            return this->TEST_system_add_task_group_concurrency;
        else
            return "-1";
    }
    string getInitial_shuffle_executor_nums()
    {
        if(readConfigFile())
            return this->initial_shuffle_executor_nums;
        else
            return "-1";
    }

    string getLog_level()
    {
        if(readConfigFile())
            return this->log_level;
        else
            return "info";
    }

    string getScript_path()
    {
        if(readConfigFile())
            return this->script_path;
        else
            return "script";
    }


    string getTask_group_close_mode()
    {
        if(readConfigFile())
            return this->task_group_close_mode;
        else
            return "true";
    }

    string getLazy_task_group_close_mode()
    {
        if(readConfigFile())
            return this->lazy_task_group_close_mode;
        else
            return "false";
    }
    string ifDoNotUseStorageNode()
    {
        if(readConfigFile())
            return this->doNotUseStorageNode;
        else
            return "false";

    }

    string getInitial_plan_used_nodes()
    {
        if(readConfigFile())
            return this->intial_plan_used_nodes;
        else
            return "false";
    }

    string getTpch_AutoGen_ScaleFactor()
    {
        if(readConfigFile())
            return this->tpch_AutoGen_ScaleFactor;
        else
            return "0";
    }

    string getAutoTuneByPlan()
    {
        if(readConfigFile())
            return this->autoTuneByPlan;
        else
            return "false";
    }


};





#endif //OLVP_EXECUTIONCONFIG_HPP
