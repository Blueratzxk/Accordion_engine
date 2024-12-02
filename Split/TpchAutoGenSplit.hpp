//
// Created by zxk on 11/30/24.
//

#ifndef OLVP_TPCHAUTOGENSPLIT_HPP
#define OLVP_TPCHAUTOGENSPLIT_HPP


#include "../Split/ConnectorSplit.hpp"
#include "../Connector/TpchTableHandle.hpp"
class TpchAutoGenSplit : public ConnectorSplit
{
    std::shared_ptr<TpchTableHandle> handle;
    int SplitTupleCount;
    int SplitOffset;
    int scanBatchSize;
    int scaleFactor;
    list<string> hosts;
    string partitionAddr = "NULL";
public:
    TpchAutoGenSplit(std::shared_ptr<TpchTableHandle> tableHandle, list<string> hosts,string partitionAddr,int SplitTupleCount,int SplitOffset,int scanBatchSize,int scaleFactor): ConnectorSplit("TpchAutoGenSplit")
    {
        this->handle = tableHandle;
        this->SplitTupleCount = SplitTupleCount;
        this->SplitOffset = SplitOffset;
        this->scanBatchSize = scanBatchSize;
        this->scaleFactor = scaleFactor;
        this->hosts = hosts;
        this->partitionAddr = partitionAddr;

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

    std::shared_ptr<TpchTableHandle> getTableHandle()
    {
        return this->handle;
    }
    int getSplitTupleCount(){return this->SplitTupleCount;}
    int getSplitOffset(){return this->SplitOffset;}
    int getScanBatchSize(){return this->scanBatchSize;}
    int getScaleFactor(){return this->scaleFactor;}



    static string Serialize(TpchAutoGenSplit tpchSplit)
    {
        nlohmann::json tpch;
        tpch["handle"] = TpchTableHandle::Serialize(*(tpchSplit.handle));
        tpch["hosts"] = tpchSplit.hosts;
        tpch["partitionAddr"] = tpchSplit.partitionAddr;
        tpch["SplitTupleCount"] = tpchSplit.SplitTupleCount;
        tpch["SplitOffset"] = tpchSplit.SplitOffset;
        tpch["scanBatchSize"] = tpchSplit.scanBatchSize;
        tpch["scaleFactor"] = tpchSplit.scaleFactor;
        string result = tpch.dump();
        return result;
    }

    static shared_ptr<TpchAutoGenSplit> Deserialize(string tpchSplit)
    {
        nlohmann::json tpch = nlohmann::json::parse(tpchSplit);

        return make_shared<TpchAutoGenSplit>(TpchTableHandle::Deserialize(tpch["handle"]),tpch["hosts"],tpch["partitionAddr"],tpch["SplitTupleCount"],
                                             tpch["SplitOffset"],tpch["scanBatchSize"],tpch["scaleFactor"]);
    }



};





#endif //OLVP_TPCHAUTOGENSPLIT_HPP
