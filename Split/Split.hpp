//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_SPLIT_HPP
#define OLVP_SPLIT_HPP

#include "../Connector/ConnectorId.hpp"
#include "ConnectorSplit.hpp"

class Split
{
    ConnectorId connectorId;
    std::shared_ptr<ConnectorSplit> connectorSplit;

public:
    Split(ConnectorId connectorId,std::shared_ptr<ConnectorSplit> connectorSplit)
    {
        this->connectorId = connectorId;
        this->connectorSplit = connectorSplit;
    }
    ConnectorId const getConnectorId()
    {
        return this->connectorId;
    }
    std::shared_ptr<ConnectorSplit> getConnectorSplit()
    {
        return this->connectorSplit;
    }
};


#endif //OLVP_SPLIT_HPP
