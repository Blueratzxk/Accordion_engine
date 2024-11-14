//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PARTITIONING_HPP
#define OLVP_PARTITIONING_HPP



#include "PartitioningHandle.hpp"
#include "PartitioningHandleSerializer.hpp"
class Partitioning
{
    shared_ptr<PartitioningHandle> handle;
    vector<string> partitionRows;

public:
    Partitioning(){}

    static shared_ptr<Partitioning> create(shared_ptr<PartitioningHandle> handle,vector<string> arguments)
    {
        return make_shared<Partitioning>(handle,arguments);
    }

    Partitioning(shared_ptr<PartitioningHandle> handle,vector<string> arguments){
        this->handle = handle;
        this->partitionRows = arguments;
    }

    shared_ptr<PartitioningHandle> getHandle()
    {
        return this->handle;
    }

    string Serialize()
    {
        nlohmann::json partitioningJson;
        string handleString = PartitioningHandleSerializer::Serialize(this->handle);
        partitioningJson["handle"] = handleString;
        partitioningJson["partitionRows"] = this->partitionRows;
        string result = partitioningJson.dump();
        return result;
    }
    shared_ptr<Partitioning> Deserialize(string partitioningStr)
    {
        nlohmann::json partitioningJson = nlohmann::json::parse(partitioningStr);

        string handleString = partitioningJson["handle"];


        shared_ptr<PartitioningHandle> pHandle = PartitioningHandleSerializer::Deserialize(handleString);
        return make_shared<Partitioning>(pHandle, partitioningJson["partitionRows"]);
    }




};

#endif //OLVP_PARTITIONING_HPP
