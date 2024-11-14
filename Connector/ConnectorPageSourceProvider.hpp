//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_CONNECTORPAGESOURCEPROVIDER_HPP
#define OLVP_CONNECTORPAGESOURCEPROVIDER_HPP

//#include "ConnectorPageSource.hpp"
class ConnectorPageSource;
class ConnectorSplit;
//#include "../Split/ConnectorSplit.hpp"

//#include "../Session/Session.hpp"
class Session;
class ConnectorPageSourceProvider
{
public:
    virtual std::shared_ptr<ConnectorPageSource> createPageSource(shared_ptr<Session> session,std::shared_ptr<ConnectorSplit> split){

        spdlog::warn("unimplemented pageSource method!");
        return NULL;
    }
};

#endif //OLVP_CONNECTORPAGESOURCEPROVIDER_HPP
