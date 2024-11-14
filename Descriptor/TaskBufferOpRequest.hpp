//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_TASKBUFFEROPREQUEST_HPP
#define OLVP_TASKBUFFEROPREQUEST_HPP


#include <nlohmann/json.hpp>
using namespace std;

class TaskBufferOpRequest
{
    string type;
    vector<string> bufferIds;

public:
    TaskBufferOpRequest(string type,vector<string> bufferIds){
        this->type = type;
        this->bufferIds = bufferIds;
    }

    string getType()
    {
        return this->type;
    }

    vector<string> getBufferIds()
    {
        return this->bufferIds;
    }

    static string Serialize(TaskBufferOpRequest update)
    {
        nlohmann::json bufferOperatingRequest;
        bufferOperatingRequest["type"] = update.type;
        bufferOperatingRequest["bufferIds"] = update.bufferIds;

        string result = bufferOperatingRequest.dump();
        return result;

    }
    static TaskBufferOpRequest Deserialize(string update)
    {
        nlohmann::json updateRequest;
        updateRequest = nlohmann::json::parse(update);
        TaskBufferOpRequest opRequest(updateRequest["type"],updateRequest["bufferIds"]);
        return opRequest;
    }


};


#endif //OLVP_TASKBUFFEROPREQUEST_HPP
