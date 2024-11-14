//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_CONNECTORPARTITIONINGHANDLE_HPP
#define OLVP_CONNECTORPARTITIONINGHANDLE_HPP


#include <string>
#include <memory>
using namespace std;

class ConnectorPartitioningHandle
{
    string connectorPatitioningId;
public:
    ConnectorPartitioningHandle(string id)
    {
        this->connectorPatitioningId = id;
    }
    string getHandleId()
    {
        return this->connectorPatitioningId;
    }
    virtual string Serialize() = 0 ;
    virtual std::shared_ptr<ConnectorPartitioningHandle> Deserialize(string handle) = 0;
    virtual bool isSingleNode(){return false;}
    virtual bool isCoordinatorOnly(){return false;}
    virtual ~ConnectorPartitioningHandle(){}
};



#endif //OLVP_CONNECTORPARTITIONINGHANDLE_HPP
