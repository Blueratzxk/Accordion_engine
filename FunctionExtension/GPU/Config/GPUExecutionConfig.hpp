//
// Created by zxk on 8/22/25.
//

#ifndef OLVP_GPUEXECUTIONCONFIG_HPP
#define OLVP_GPUEXECUTIONCONFIG_HPP



#include "nlohmann/json.hpp"
#include <string>
#include <iostream>
#include <fstream>
//#include "../Utils/Random.hpp"

using namespace std;

class GPUExecutionConfig
{

    string GPUBatchAssembleThreshold = "1073741824"; //1GB
    string UseGPUBatchAssembleOperator = "true";

public:
    GPUExecutionConfig(){
        readConfigFile();
    }

    void readConfigFile() {


        string strFileData = "GPU_execution.config";
        std::ifstream in(strFileData);

        if(in.is_open()) {
            nlohmann::json jsonTree = nlohmann::json::parse(in);
            this->GPUBatchAssembleThreshold = jsonTree["GPUBatchAssembleThreshold"];
            this->UseGPUBatchAssembleOperator = jsonTree["UseGPUBatchAssembleOperator"];
        }

        in.close();


    }

    long getGPUBatchAssembleThreshold()
    {
        return atol(this->GPUBatchAssembleThreshold.c_str());
    }

    bool isUseGPUBatchAssembleOperator()
    {
        if(this->UseGPUBatchAssembleOperator == "true")
            return true;
        else
            return false;
    }
};




#endif //OLVP_GPUEXECUTIONCONFIG_HPP
