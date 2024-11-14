//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_NODEPARTITIONINGMANAGER_HPP
#define OLVP_NODEPARTITIONINGMANAGER_HPP


#include "../NodeCluster/Node.hpp"
#include "NodePartitionMap.hpp"
#include "PartitioningHandle.hpp"
#include "SystemPartitioningHandle.h"
class NodePartitioningManager
{

    NodeSelector aSelector;

public:
    NodePartitioningManager()
    {

    }

    NodePartitionMap getNodePartitioningMap(shared_ptr<PartitioningHandle> handle)
    {
        if(handle == NULL || handle->getConnectorHandle() == NULL)//here should be used to manage sourcePartitioningHandle
        {
            // return ((systemPartitioningHandle*)((handle).getConnectorHandle()))->getNodePartitionMap(aSelector);
            return NodePartitionMap(aSelector.getNodesByMinThreadNums(1));
        }
        if(handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0)
        {
            return (static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle()))->getNodePartitionMap(aSelector);
        }

        return NodePartitionMap(aSelector.getNodesByMinThreadNums(1));
    }



};



#endif //OLVP_NODEPARTITIONINGMANAGER_HPP
