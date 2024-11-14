//
// Created by zxk on 11/26/23.
//

#ifndef OLVP_HEARTBEAT_HPP
#define OLVP_HEARTBEAT_HPP

#include "nlohmann/json.hpp"
using namespace std;
class Heartbeat
{
    string nodeHttpURL;
    int activeTaskNums;
    int activeThreadNums;
    int nodeCpuCoreNums;
    float nodeCpuUsage;
    bool hasStorage;
    double net_receiveRate;
    double net_transRate;
    int net_speed;

public:
    Heartbeat(string nodeHttpURL,int activeTaskNums,int activeThreadNums, int nodeCpuCoreNums,float nodeCpuUsage,bool hasStorage,double net_receiveRate,double net_transRate,int net_speed)
    {
        this->nodeHttpURL = nodeHttpURL;

        this->activeTaskNums = activeTaskNums;
        this->activeThreadNums = activeThreadNums;
        this->hasStorage = hasStorage;
        this->nodeCpuCoreNums = nodeCpuCoreNums;
        this->nodeCpuUsage = nodeCpuUsage;
        this->net_receiveRate = net_receiveRate;
        this->net_transRate = net_transRate;
        this->net_speed = net_speed;
    }

    string getNodeHttpURL(){return this->nodeHttpURL;}

    int getActiveTaskNums(){return this->activeTaskNums;}
    int getActiveThreadNums(){return this->activeThreadNums;}
    int getNodeCpuCoreNums(){return this->nodeCpuCoreNums;}
    float getNodeCpuUsage(){return this->nodeCpuUsage;}
    double getNet_receiveRate(){return this->net_receiveRate;}
    double getNet_transRate(){return this->net_transRate;}
    int getNet_speed(){return this->net_speed;}

    bool ifHasStorgae(){return this->hasStorage;}
    static string Serialize(Heartbeat heartbeat)
    {
        nlohmann::json json;

        json["nodeHttpURL"] = heartbeat.nodeHttpURL;

        json["activeTaskNums"] = heartbeat.activeTaskNums;
        json["activeThreadNums"] = heartbeat.activeThreadNums;
        json["nodeCpuCoreNums"] = heartbeat.nodeCpuCoreNums;
        json["nodeCpuUsage"] = heartbeat.nodeCpuUsage;
        json["hasStorage"] = heartbeat.hasStorage;
        json["net_receiveRate"] = heartbeat.net_receiveRate;
        json["net_transRate"] = heartbeat.net_transRate;
        json["net_speed"] = heartbeat.net_speed;
        return json.dump();
    }
    static Heartbeat Deserialize(string heartbeat)
    {
        nlohmann::json json = nlohmann::json::parse(heartbeat);
        return Heartbeat(json["nodeHttpURL"],json["activeTaskNums"],json["activeThreadNums"],json["nodeCpuCoreNums"],
                         json["nodeCpuUsage"],json["hasStorage"],json["net_receiveRate"],json["net_transRate"],json["net_speed"]);
    }


};


#endif //OLVP_HEARTBEAT_HPP
