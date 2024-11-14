//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PARTITIONINGHANDLE_HPP
#define OLVP_PARTITIONINGHANDLE_HPP


#include "../Connector/ConnectorPartitioningHandle.hpp"
#include "nlohmann/json.hpp"


class PartitioningHandle
{
    string connectorId = "NULL";
    std::shared_ptr<ConnectorPartitioningHandle> connectorHandle = NULL;

public:
    PartitioningHandle(){}

    /*
    ~partitioningHandle(){
        if(this->connectorHandle != NULL)
        {
            delete connectorHandle;
        }
    }
    */

    PartitioningHandle(string connectorId,std::shared_ptr<ConnectorPartitioningHandle> connectorHandle)
    {
        this->connectorId = connectorId;
        this->connectorHandle = connectorHandle;
    }
    PartitioningHandle(std::shared_ptr<ConnectorPartitioningHandle> connectorHandle)
    {
        this->connectorHandle = connectorHandle;
    }

    std::shared_ptr<ConnectorPartitioningHandle> getConnectorHandle()
    {
        return connectorHandle;
    }
    string getConnectorId()
    {
        return this->connectorId;
    }

    bool isSingleNode()
    {
        return connectorHandle->isSingleNode();
    }

    bool isCoordinatorOnly()
    {
        return connectorHandle->isCoordinatorOnly();
    }
    bool equals(PartitioningHandle handle)
    {
        if(this->connectorHandle == handle.connectorHandle && this->connectorId.compare(handle.connectorId) == 0)
        {
            return true;
        }
        else
            return false;
    }

    static string Serialize(PartitioningHandle handle)
    {
        nlohmann::json ph;
        ph["connectorId"] = handle.getConnectorId();
        ph["handleId"] = handle.getConnectorHandle()->getHandleId();
        ph["connectorPartitioningHandle"] = handle.connectorHandle->Serialize();
        string result = ph.dump();
        return result;
    }


};


#endif //OLVP_PARTITIONINGHANDLE_HPP
