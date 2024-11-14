//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_PLANNODEID_HPP
#define OLVP_PLANNODEID_HPP

#include <string>
class PlanNodeId
{
    std::string id = "NULL";

public:
    PlanNodeId(){}
    PlanNodeId(std::string id)
    {
        this->id = id;
    }

    std::string get()
    {
        return this->id;
    }

    bool operator<(const PlanNodeId &p) const //注意这里的两个const
    {
        return id < p.id ;
    }

    static string Serialize(PlanNodeId planNodeId)
    {
        nlohmann::json json;

        json["id"] = planNodeId.id;

        string result = json.dump();
        return result;
    }
    static shared_ptr<PlanNodeId> Deserialize(string planNodeId)
    {
        nlohmann::json json = nlohmann::json::parse(planNodeId);
        return make_shared<PlanNodeId>(json["id"]);
    }



};


#endif //OLVP_PLANNODEID_HPP
