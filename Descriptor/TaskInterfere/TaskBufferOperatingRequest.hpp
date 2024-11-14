//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_TASKBUFFEROPERATINGREQUEST_HPP
#define OLVP_TASKBUFFEROPERATINGREQUEST_HPP

#include "nlohmann/json.hpp"
#include "TaskInterfereRequest.hpp"
using namespace std;

class TaskBufferOperatingRequest : public TaskInterfereRequest
{

public:
    enum opType
    {
        CLOSE_BUFFER,
        CLOSE_BUFFER_GROUP,
    };

private:

    opType type;
    vector<string> bufferIds;


public:
    TaskBufferOperatingRequest(TaskBufferOperatingRequest::opType type,vector<string> bufferIds):TaskInterfereRequest("TaskBufferOperatingRequest"){
        this->type = type;
        this->bufferIds = bufferIds;
    }

    TaskBufferOperatingRequest::opType getType()
    {
        return this->type;
    }

    vector<string> getBufferIds()
    {
        return this->bufferIds;
    }

    static string Serialize(TaskBufferOperatingRequest update)
    {
        nlohmann::json bufferOperatingRequest;
        bufferOperatingRequest["type"] = update.type;
        bufferOperatingRequest["bufferIds"] = update.bufferIds;

        string result = bufferOperatingRequest.dump();
        return result;

    }
    static shared_ptr<TaskBufferOperatingRequest> Deserialize(string update)
    {
        nlohmann::json updateRequest;
        updateRequest = nlohmann::json::parse(update);
        auto opRequest = make_shared<TaskBufferOperatingRequest>(updateRequest["type"],updateRequest["bufferIds"]);
        return opRequest;
    }


};



#endif //OLVP_TASKBUFFEROPERATINGREQUEST_HPP
