//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_TASKSOURCE_HPP
#define OLVP_TASKSOURCE_HPP


#include "../../Frontend/PlanNode/PlanNodeId.hpp"
#include "ScheduledSplit.hpp"

class TaskSource
{
    PlanNodeId planNodeId;
    std::set<std::shared_ptr<ScheduledSplit>> splits;

public:

    TaskSource( PlanNodeId planNodeId,std::set<std::shared_ptr<ScheduledSplit>> splits)
    {
        this->planNodeId = planNodeId;
        this->splits = splits;
    }

    PlanNodeId getPlanNodeId()
    {
        return this->planNodeId;
    }
    std::set<std::shared_ptr<ScheduledSplit>> getSplits()
    {
        return this->splits;
    }

    void addSplit(std::shared_ptr<ScheduledSplit> split)
    {
        this->splits.insert(split);
    }

    static shared_ptr<TaskSource> getEmptyTaskSource()
    {
        std::set<std::shared_ptr<ScheduledSplit>> emptySplits;
        return make_shared<TaskSource>(PlanNodeId("-1"),emptySplits);
    }

    static string Serialize(TaskSource taskSource)
    {
        nlohmann::json json;

        json["planNodeId"] = PlanNodeId::Serialize(taskSource.planNodeId);

        set<string> splitStrings;
        for(auto split : taskSource.splits)
        {
            splitStrings.insert(ScheduledSplit::Serialize(*split));
        }

        json["splits"] = splitStrings;

        string result = json.dump();
        return result;
    }
    static shared_ptr<TaskSource> Deserialize(string taskSource)
    {

        if(taskSource == "NULL")
            return NULL;

        nlohmann::json json = nlohmann::json::parse(taskSource);
        std::set<std::shared_ptr<ScheduledSplit>> splits;
        set<string> splitStrings = json["splits"];
        for(auto splitStr : splitStrings)
        {
            splits.insert(ScheduledSplit::Deserialize(splitStr));
        }

        return make_shared<TaskSource>(*PlanNodeId::Deserialize(json["planNodeId"]),splits);
    }



};


#endif //OLVP_TASKSOURCE_HPP
