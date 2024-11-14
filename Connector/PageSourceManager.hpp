//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_PAGESOURCEMANAGER_HPP
#define OLVP_PAGESOURCEMANAGER_HPP

#include "ConnectorId.hpp"
#include "ConnectorPageSourceProvider.hpp"
#include "tbb/concurrent_hash_map.h"
//#include "ConnectorPageSource.hpp"
class ConnectorPageSource;
#include "../Split/Split.hpp"


typedef tbb::concurrent_hash_map<ConnectorId,std::shared_ptr<ConnectorPageSourceProvider>,ConnectorId::compare> pageSourceProvidersHashMap;

class PageSourceManager
{
    tbb::concurrent_hash_map<ConnectorId,std::shared_ptr<ConnectorPageSourceProvider>,ConnectorId::compare> pageSourceProviders;

public:

    void addConnectorPageSourceProvider(ConnectorId connectorId,std::shared_ptr<ConnectorPageSourceProvider> provider)
    {
        tbb::concurrent_hash_map<ConnectorId,std::shared_ptr<ConnectorPageSourceProvider>,ConnectorId::compare>::value_type hashMapValuePair(connectorId,provider);
        this->pageSourceProviders.insert(hashMapValuePair);
    }
    void removeConnectorPageSourceProvider(ConnectorId connectorId)
    {
        this->pageSourceProviders.erase(connectorId);
    }

    std::shared_ptr<ConnectorPageSourceProvider> getPageSourceProvider(Split split)
    {
        for(pageSourceProvidersHashMap::iterator iterator1 = pageSourceProviders.begin(); iterator1 != pageSourceProviders.end(); ++iterator1 ){

            if(ConnectorId::compare::equal(iterator1->first,split.getConnectorId())) {
                return iterator1->second;
            }
        }
        spdlog::critical("Cannot find this catalog "+split.getConnectorId().getCatalogName()+"!");
        return NULL;
    }

    std::shared_ptr<ConnectorPageSource> createPageSource(shared_ptr<Session> session,Split split)
    {
        if(this->getPageSourceProvider(split) != NULL) {
            return this->getPageSourceProvider(split)->createPageSource(session,split.getConnectorSplit());
        }
        else
            return NULL;
    }

    void view()
    {
        for(auto elem : this->pageSourceProviders)
        {
            cout << elem.first.getCatalogName()<<endl;
        }

    }

};

#endif //OLVP_PAGESOURCEMANAGER_HPP
