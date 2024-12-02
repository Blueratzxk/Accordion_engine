//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SPLITSOURCEFACTORY_HPP
#define OLVP_SPLITSOURCEFACTORY_HPP



#include "../Planner/Fragment.hpp"
#include "../Frontend/PlanNode/PlanNode.hpp"
#include "Split.hpp"
#include "../DataSource/SchemaManager.hpp"

class SplitSourceFactory
{

public:
    SplitSourceFactory(){

    }
    map<string,vector<shared_ptr<Split>>> createSplitSources(PlanFragment fragment)
    {
        map<string,vector<shared_ptr<Split>>> splitSources;
        findLocalSourceNode(fragment.getRoot(),splitSources);
        return splitSources;
    }

    void findLocalSourceNode(PlanNode *root,map<string,vector<shared_ptr<Split>>> &splitSources)
    {
        vector<PlanNode*> planNodes = root->getSources();
        for(int i = 0 ; i < planNodes.size() ; i++)
        {
            findLocalSourceNode(planNodes[i],splitSources);
        }

        if(root->getType().compare("TableScanNode") == 0)
        {
            TableScanNode *tableScanNode = (TableScanNode *)root;

            string catalog = tableScanNode->getDescriptor().getCatalog();
            string schema = tableScanNode->getDescriptor().getSchema();
            string table = tableScanNode->getDescriptor().getTable();

            std::shared_ptr<TpchTableHandle> tableHandle2 = std::make_shared<TpchTableHandle>(catalog,schema,table);
            list<string> hosts{};

            CatalogsMetaManager manager;
            vector<pair<string,string>> pfs = manager.getTable(catalog,schema,table).getPartiionedFilePaths();
            string defaultScanSize = manager.getTable(catalog,schema,table).getDefaultBlockScanSize();


            ExecutionConfig executionConfig;
            string sf = executionConfig.getTpch_AutoGen_ScaleFactor();
            int scaleFactor = atoi(sf.c_str());

            if(pfs.size() > 0)
            {
                if(scaleFactor > 0)
                {
                    produceAutoGenSplits(tableHandle2,(TableScanNode *)root,pfs,atoi(defaultScanSize.c_str()),scaleFactor,splitSources);
                    return;
                }

                for(auto host : pfs)
                {
                    list<string> addrs;
                    addrs.push_back(host.first);
                    std::shared_ptr<TpchSplit> tpchSplit2 = std::make_shared<TpchSplit>(tableHandle2, addrs,host.second,defaultScanSize);
                    auto split2 = make_shared<Split>(ConnectorId(catalog), tpchSplit2);
                    splitSources[root->getId()].push_back(split2);
                }
            }
            else {
                std::shared_ptr<TpchSplit> tpchSplit2 = std::make_shared<TpchSplit>(tableHandle2, hosts,defaultScanSize);
                auto split2 = make_shared<Split>(ConnectorId(catalog), tpchSplit2);
                splitSources[root->getId()].push_back(split2);
            }
        }

    }

    void produceAutoGenSplits(std::shared_ptr<TpchTableHandle> tableHandle,TableScanNode *root,vector<pair<string,string>> pfs,
                              int defaultScanSize, int scaleFactor,
                              map<string,vector<shared_ptr<Split>>> &splitSources)
    {

        string tableName = tableHandle->getTableName();
        if(tableName == "lineitem" || tableName == "LINEITEM")
            tableName = "orders";
        unsigned long rowCount = facebook::velox::tpch::getRowCount(tableName,scaleFactor);

        int splitSize = rowCount / pfs.size();
        vector<int> offsets;
        vector<int> splitSizes;
        int curSize = 0;
        for(int i = 0 ; i < pfs.size() ; i++)
        {
            if(i < pfs.size() - 1) {
                auto offset = i * splitSize;
                splitSizes.push_back(splitSize);
                offsets.push_back(offset);
            }
            else
            {
                auto offset = i * splitSize;
                splitSizes.push_back(rowCount - curSize);
                offsets.push_back(offset);
            }
            curSize += splitSize;
        }

        int index = 0;
        int scanBatchSize = 10000;
        for(auto host : pfs)
        {
            list<string> addrs;
            addrs.push_back(host.first);
            std::shared_ptr<TpchAutoGenSplit> tpchSplit2 = std::make_shared<TpchAutoGenSplit>(tableHandle, addrs,host.second,
                                                                                              splitSizes[index],offsets[index],defaultScanSize,scaleFactor);

            index++;
            auto split2 = make_shared<Split>(ConnectorId(root->getDescriptor().getCatalog()), tpchSplit2);
            splitSources[root->getId()].push_back(split2);
        }
    }

};

#endif //OLVP_SPLITSOURCEFACTORY_HPP
