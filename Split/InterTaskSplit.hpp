//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKSPLIT_HPP
#define OLVP_INTERTASKSPLIT_HPP



#include "../Split/ConnectorSplit.hpp"
#include "../Connector/TpchTableHandle.hpp"
#include "../Execution/Task/Id/TaskId.hpp"
#include "WebLocation.hpp"
class InterTaskSplit : public ConnectorSplit
{
    std::shared_ptr<TaskId> taskId;
    string componentId;
    std::shared_ptr<Location> location;

public:
    InterTaskSplit(std::shared_ptr<TaskId> taskId,string componentId,std::shared_ptr<Location> location): ConnectorSplit("InterTaskSplit")
    {
        this->taskId = taskId;
        this->componentId = componentId;
        this->location = location;
    }
    std::shared_ptr<TaskId> getTaskId()
    {
        return this->taskId;
    }
    std::shared_ptr<Location> getLocation()
    {
        return this->location;
    }
    string getComponentId()
    {
        return this->componentId;
    }


    static string Serialize(InterTaskSplit interTaskSplit)
    {
        nlohmann::json json;
        json["taskId"] = TaskId::Serialize(*interTaskSplit.taskId);
        json["location"] = Location::Serialize(*interTaskSplit.location);
        json["componentId"] = interTaskSplit.componentId;

        string result = json.dump();
        return result;
    }
    static shared_ptr<InterTaskSplit> Deserialize(string remoteSplit)
    {
        nlohmann::json json = nlohmann::json::parse(remoteSplit);
        return make_shared<InterTaskSplit>(TaskId::Deserialize(json["taskId"]),json["componentId"],Location::Deserialize(json["location"]));
    }




};


#endif //OLVP_INTERTASKSPLIT_HPP
