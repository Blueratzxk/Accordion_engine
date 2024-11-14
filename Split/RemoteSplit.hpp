//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_REMOTESPLIT_HPP
#define OLVP_REMOTESPLIT_HPP


#include "../Split/ConnectorSplit.hpp"
#include "../Connector/TpchTableHandle.hpp"
#include "../Execution/Task/Id/TaskId.hpp"
#include "WebLocation.hpp"
class RemoteSplit : public ConnectorSplit
{
    std::shared_ptr<TaskId> taskId;
    std::shared_ptr<Location> location;

public:
    RemoteSplit(  std::shared_ptr<TaskId> taskId,std::shared_ptr<Location> location): ConnectorSplit("RemoteSplit")
    {
        this->taskId = taskId;
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


    static string Serialize(RemoteSplit remoteSplit)
    {
        nlohmann::json json;
        json["taskId"] = TaskId::Serialize(*remoteSplit.taskId);
        json["location"] = Location::Serialize(*remoteSplit.location);

        string result = json.dump();
        return result;
    }
    static shared_ptr<RemoteSplit> Deserialize(string remoteSplit)
    {
        nlohmann::json json = nlohmann::json::parse(remoteSplit);
        return make_shared<RemoteSplit>(TaskId::Deserialize(json["taskId"]),Location::Deserialize(json["location"]));
    }




};

#endif //OLVP_REMOTESPLIT_HPP
