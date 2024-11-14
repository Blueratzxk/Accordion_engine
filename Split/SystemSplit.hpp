//
// Created by zxk on 5/17/23.
//

#ifndef OLVP_SYSTEMSPLIT_HPP
#define OLVP_SYSTEMSPLIT_HPP

#include "../Connector/ConnectorId.hpp"
#include "../Split/ConnectorSplit.hpp"

class SystemSplit : public ConnectorSplit
{
    ConnectorId connectorId;

public:
    SystemSplit(ConnectorId connectorId): ConnectorSplit("SystemSplit")
    {
        this->connectorId = connectorId;
    }
    ConnectorId getConnectorId()
    {
        return this->connectorId;
    }

};


#endif //OLVP_SYSTEMSPLIT_HPP
