//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_AGGREGATEDESCRIPTOR_HPP
#define OLVP_AGGREGATEDESCRIPTOR_HPP

#include <string>
#include "nlohmann/json.hpp"
using namespace std;

class AggregateDesc
{
    string functionName;
    string inputKey;
    string outputName;
public:
    AggregateDesc(string functionName,string inputKey,string outputName){
        this->functionName = functionName;
        this->inputKey = inputKey;
        this->outputName = outputName;
    }

    string getFunctionName()
    {
        return this->functionName;
    }
    string getInputKey()
    {
        return this->inputKey;
    }
    string getOutputName()
    {
        return this->outputName;
    }

    static string Serialize(AggregateDesc aggregateDesc)
    {
        nlohmann::json desc;

        desc["functionName"] = aggregateDesc.functionName;
        desc["inputKey"] = aggregateDesc.inputKey;
        desc["outputName"] = aggregateDesc.outputName;

        string result = desc.dump();

        return result;
    }
    static AggregateDesc Deserialize(string desc)
    {

        nlohmann::json agg = nlohmann::json::parse(desc);
        return AggregateDesc(agg["functionName"],agg["inputKey"],agg["outputName"]);
    }

    string to_string()
    {
        string result;


        result=result+"functionName:"+this->functionName+"|"+"inputKey:"+this->inputKey+"|"+"outputName"+this->outputName+"|";

        return result;
    }


};
#endif //OLVP_AGGREGATEDESCRIPTOR_HPP
