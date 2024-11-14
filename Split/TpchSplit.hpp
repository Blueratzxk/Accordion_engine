//
// Created by zxk on 5/17/23.
//

#ifndef OLVP_TPCHSPLIT_HPP
#define OLVP_TPCHSPLIT_HPP

#include "../Split/ConnectorSplit.hpp"
#include "../Connector/TpchTableHandle.hpp"
class TpchSplit : public ConnectorSplit
{
    std::shared_ptr<TpchTableHandle> handle;
    list<string> hosts;
    string partitionAddr = "NULL";
    string defaultScanSize = "-1";
public:
    TpchSplit(std::shared_ptr<TpchTableHandle> tableHandle,list<string> hosts,string defaultScanSize): ConnectorSplit("TpchSplit")
    {
        this->handle = tableHandle;
        this->hosts = hosts;
        this->defaultScanSize = defaultScanSize;
    }
    TpchSplit(std::shared_ptr<TpchTableHandle> tableHandle,list<string> hosts,string partitionAddr,string defaultScanSize): ConnectorSplit("TpchSplit")
    {
        this->handle = tableHandle;
        this->hosts = hosts;
        this->partitionAddr = partitionAddr;
        this->defaultScanSize = defaultScanSize;
    }

    std::shared_ptr<TpchTableHandle> getTableHandle()
    {
        return this->handle;
    }
    string getDefaultScanSize()
    {
        return this->defaultScanSize;
    }

    list<string> getPreferredNodes(){

        return this->hosts;
    }

    bool hasPartitionAddr()
    {
        return this->partitionAddr != "NULL";
    }
    string getPartitionAddr()
    {
        return this->partitionAddr;
    }

    static string Serialize(TpchSplit tpchSplit)
    {
        nlohmann::json tpch;
        tpch["handle"] = TpchTableHandle::Serialize(*(tpchSplit.handle));
        tpch["hosts"] = tpchSplit.hosts;
        tpch["partitionAddr"] = tpchSplit.partitionAddr;
        tpch["defaultScanSize"] = tpchSplit.defaultScanSize;
        string result = tpch.dump();
        return result;
    }

    static shared_ptr<TpchSplit> Deserialize(string tpchSplit)
    {
        nlohmann::json tpch = nlohmann::json::parse(tpchSplit);

        return make_shared<TpchSplit>(TpchTableHandle::Deserialize(tpch["handle"]),tpch["hosts"],tpch["partitionAddr"],tpch["defaultScanSize"]);
    }



};


#endif //OLVP_TPCHSPLIT_HPP
