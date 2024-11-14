//
// Created by zxk on 6/2/23.
//

#ifndef OLVP_SCHEDULEDSPLIT_HPP
#define OLVP_SCHEDULEDSPLIT_HPP

#include "../../Split/Split.hpp"
#include "../../Frontend/PlanNode/PlanNodeId.hpp"
#include "../../Split/Serial/SplitSerial.hpp"
class ScheduledSplit
{
    PlanNodeId planNodeId;
    std::shared_ptr<Split> split;
public:
    ScheduledSplit(PlanNodeId planNodeId,std::shared_ptr<Split> split)
    {
        this->planNodeId = planNodeId;
        this->split = split;
    }

    std::shared_ptr<Split> getSplit()
    {
        return this->split;
    }
    PlanNodeId getPlanNodeId()
    {
        return this->planNodeId;
    }


    static string Serialize(ScheduledSplit scheduledSplit)
    {
        nlohmann::json json;
        json["planNodeId"] = PlanNodeId::Serialize(scheduledSplit.planNodeId);
        json["split"] = SplitSerial::Serialize(*(scheduledSplit.split));
        string result = json.dump();
        return result;
    }
    static shared_ptr<ScheduledSplit> Deserialize(string scheduledSplit)
    {
        nlohmann::json json = nlohmann::json::parse(scheduledSplit);

        return make_shared<ScheduledSplit>(*(PlanNodeId::Deserialize(json["planNodeId"])),SplitSerial::Deserialize(json["split"]));
    }



};


#endif //OLVP_SCHEDULEDSPLIT_HPP
