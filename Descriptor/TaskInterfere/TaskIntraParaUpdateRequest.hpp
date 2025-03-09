//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_TASKINTRAPARAUPDATEREQUEST_HPP
#define OLVP_TASKINTRAPARAUPDATEREQUEST_HPP


#include "nlohmann/json.hpp"

#include "TaskInterfereRequest.hpp"
using namespace std;
class TaskIntraParaUpdateRequest : public TaskInterfereRequest
{
    string pipelineId;
    string increOrDecreParallel;
    string updateParaCount;
    string extension;

public:
    TaskIntraParaUpdateRequest(string pipelineId,string increOrDecre,string updataParaCount,string extension = "NO") : TaskInterfereRequest("TaskIntraParaUpdateRequest"){
        this->pipelineId = pipelineId;
        this->increOrDecreParallel = increOrDecre;
        this->updateParaCount = updataParaCount;
        this->extension = extension;
    }

    string getPipelineId()
    {
        return this->pipelineId;
    }
    string getUpdateType()
    {
        return this->increOrDecreParallel;
    }
    string getUpdateParaCount()
    {
        return this->updateParaCount;
    }
    bool isExtension()
    {
        return this->extension != "NO";
    }
    string getExtension()
    {
        return this->extension;
    }

    static string Serialize(TaskIntraParaUpdateRequest update)
    {
        nlohmann::json IntraParaUpdateRequest;
        IntraParaUpdateRequest["pipelineId"] = update.pipelineId;
        IntraParaUpdateRequest["updateType"] = update.increOrDecreParallel;
        IntraParaUpdateRequest["updateCount"] = update.updateParaCount;
        IntraParaUpdateRequest["extension"] = update.extension;
        string result = IntraParaUpdateRequest.dump();
        return result;

    }
    static shared_ptr<TaskIntraParaUpdateRequest> Deserialize(string update)
    {
        nlohmann::json updateRequest;
        updateRequest = nlohmann::json::parse(update);
        auto intraRequest = make_shared<TaskIntraParaUpdateRequest>(updateRequest["pipelineId"],updateRequest["updateType"],
                                                                    updateRequest["updateCount"],updateRequest["extension"]);
        return intraRequest;
    }


};



#endif //OLVP_TASKINTRAPARAUPDATEREQUEST_HPP
