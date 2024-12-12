//
// Created by zxk on 11/26/23.
//

#ifndef OLVP_CLUSTERSERVER_H
#define OLVP_CLUSTERSERVER_H


#include "../NodeCluster/NodesManager.h"
#include "../Web/Restful/Client.hpp"
#include "../Execution/Task/Statistics/NIC/NetInfoCollector.hpp"

using namespace  std;
class ClusterServer
{
    static shared_ptr<NodesManager> nodesManager;
    static int heartbeatFreq;
    static bool useStorage;
    static shared_ptr<NetInfoCollector> netInfoCollector;
    static bool showInfos;
    static shared_ptr<RestfulClient> restfulClient;
public:
    ClusterServer();

    static void start();
    static shared_ptr<NodesManager> getNodesManager();
    static void resolveHeartbeat(string heartbeat);
    static void sendHeartbeat();
    static void heartbeatSender();
    static void startHeartbeat();
    static shared_ptr<NetInfoCollector> getNetInfoCollector();

    static void openClusterInfoDisplay()
    {
        ClusterServer::showInfos = true;
    }
    static void closeClusterInfoDisplay()
    {
        ClusterServer::showInfos = false;
    }
};


#endif //OLVP_CLUSTERSERVER_H
