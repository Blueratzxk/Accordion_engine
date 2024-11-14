//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_STAGEID_HPP
#define OLVP_STAGEID_HPP

#include "QueryId.hpp"

class StageId
{
    QueryId queryId;
    int id;
public:
    StageId(){

    }
    StageId(QueryId queryId,int id)
    {
        this->queryId = queryId;
        this->id = id;
    }

    QueryId getQueryId()
    {
        return this->queryId;
    }
    int getId()
    {
        return this->id;
    }

    bool operator<(const StageId &p) const //注意这里的两个const
    {
        return (id < p.id) || (queryId < p.queryId);
    }
    bool operator==(const StageId &p) const //注意这里的两个const
    {
        return (id == p.id) || (queryId == p.queryId);
    }
    static string Serialize(StageId stageId)
    {
        nlohmann::json json;
        json["id"] = stageId.id;
        json["queryId"] = QueryId::Serialize(stageId.queryId);

        string result = json.dump();
        return result;
    }
    static shared_ptr<StageId> Deserialize(string stageId)
    {
        nlohmann::json json = nlohmann::json::parse(stageId);
        return make_shared<StageId>(*QueryId::Deserialize(json["queryId"]),json["id"]);
    }

};


#endif //OLVP_STAGEID_HPP
