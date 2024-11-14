//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_STAGEEXECUTIONID_HPP
#define OLVP_STAGEEXECUTIONID_HPP

#include "StageId.hpp"
class StageExecutionId
{
    StageId stageId;
    int id;

public:
    StageExecutionId(){

    }
    StageExecutionId(StageId stageId,int id)
    {
        this->stageId = stageId;
        this->id = id;
    }

    StageId getStageId()
    {
        return this->stageId;
    }
    int getId()
    {
        return this->id;
    }

    bool operator<(const StageExecutionId &p) const //注意这里的两个const
    {
        return (id < p.id) || (stageId < p.stageId);
    }

    bool operator==(const StageExecutionId &p) const //注意这里的两个const
    {
        return (id == p.id) || (stageId == p.stageId);
    }


    static string Serialize(StageExecutionId stageExecutionId)
    {
        nlohmann::json json;
        json["id"] = stageExecutionId.id;
        json["stageId"] = StageId::Serialize(stageExecutionId.stageId);

        string result = json.dump();
        return result;
    }
    static shared_ptr<StageExecutionId> Deserialize(string stageExecutionId)
    {
        nlohmann::json json = nlohmann::json::parse(stageExecutionId);
        return make_shared<StageExecutionId>(*StageId::Deserialize(json["stageId"]),json["id"]);
    }


};


#endif //OLVP_STAGEEXECUTIONID_HPP
