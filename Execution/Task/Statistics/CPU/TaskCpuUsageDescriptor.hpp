//
// Created by zxk on 7/7/24.
//

#ifndef OLVP_TASKCPUUSAGEDESCRIPTOR_HPP
#define OLVP_TASKCPUUSAGEDESCRIPTOR_HPP

#include <map>
#include <string>
#include "nlohmann/json.hpp"
class TaskCpuUsageDescriptor
{
    std::map<std::string,float> descriptor;
    std::map<std::string,int> typeNums;

public:
    TaskCpuUsageDescriptor(std::map<std::string,float> descriptor,std::map<std::string,int> typeNums){
        this->descriptor = descriptor;
        this->typeNums = typeNums;

    }

    float getShuffleCpuUsage()
    {
        return this->descriptor["shuffle"];
    }
    float getDriverCpuUsage()
    {
        return this->descriptor["driver"];
    }
    TaskCpuUsageDescriptor(){

    }
    void addInfo(std::string type,float value)
    {
        descriptor[type] = value;
    }
    void addTypeNums(std::string type,int value)
    {
        typeNums[type] = value;
    }

    int getDriverNum()
    {
        return this->typeNums["driver"];
    }
    int getShufflerNum()
    {
        return this->typeNums["shuffle"];
    }
    static std::string Serialize(TaskCpuUsageDescriptor taskCpuUsageDescriptor)
    {
        nlohmann::json desc;


        desc["descriptor"] = taskCpuUsageDescriptor.descriptor;
        desc["typeNums"] = taskCpuUsageDescriptor.typeNums;

        std::string result = desc.dump();

        return result;
    }
    static TaskCpuUsageDescriptor Deserialize(std::string desc)
    {
        nlohmann::json descriptor = nlohmann::json::parse(desc);
        return TaskCpuUsageDescriptor(descriptor["descriptor"],descriptor["typeNums"]);
    }

};

#endif //OLVP_TASKCPUUSAGEDESCRIPTOR_HPP
