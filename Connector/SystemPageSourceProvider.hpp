//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_SYSTEMPAGESOURCEPROVIDER_HPP
#define OLVP_SYSTEMPAGESOURCEPROVIDER_HPP

#include "ConnectorPageSource.hpp"
#include "ConnectorPageSourceProvider.hpp"
//#include "../Split/ConnectorSplit.hpp"
//#include "../Connector/ConnectorId.hpp"
#include "../DataSource/TpchPageSource.hpp"
#include "../Split/SystemSplit.hpp"
#include "../Split/TpchSplit.hpp"

class ConnectorSplit;

class SystemPageSourceProvider:public ConnectorPageSourceProvider
{
    std::shared_ptr<ConnectorPageSource> createPageSource(shared_ptr<Session> session,std::shared_ptr<ConnectorSplit> split)
    {

        if(split->getId().compare("SystemSplit") == 0)
        {
            std::shared_ptr<SystemSplit> sysSplit = static_pointer_cast<SystemSplit>(split);

            spdlog::warn("Unsupported ConnectorPageSource "+ split->getId());

            return NULL;
        }
        else if(split->getId().compare("TpchSplit") == 0)
        {
            std::shared_ptr<TpchSplit> tpchSplit = static_pointer_cast<TpchSplit>(split);

            string catalogName = tpchSplit->getTableHandle()->getCatalogName();
            string schemaName = tpchSplit->getTableHandle()->getSchemaName();
            string tableName = tpchSplit->getTableHandle()->getTableName();
            string defaultScanSize = tpchSplit->getDefaultScanSize();

            string localFileAddr = "NULL";
            if(tpchSplit->hasPartitionAddr())
            {
                localFileAddr = tpchSplit->getPartitionAddr();
            }

            std::shared_ptr<TpchPageSource> tpch = std::make_shared<TpchPageSource>(session,defaultScanSize);

            if(localFileAddr != "NULL")
            {
                if(tpch->load(catalogName,schemaName,tableName,localFileAddr))
                {
                    return tpch;
                }
                return NULL;
            }
            else if(tpch->load(catalogName,schemaName,tableName)) {
                return tpch;
            }
            else {
                return NULL;
            }

        }
        else
        {
            spdlog::warn("Unknown ConnectorPageSource "+ split->getId()+"!!!!");
            return NULL;

        }

    }

};


#endif //OLVP_SYSTEMPAGESOURCEPROVIDER_HPP
