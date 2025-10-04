//
// Created by zxk on 11/26/23.
//

#ifndef OLVP_CLUSTERSERVER_H
#define OLVP_CLUSTERSERVER_H


#include "../NodeCluster/NodesManager.h"
#include "../Web/Restful/Client.hpp"
#include "../Execution/Task/Statistics/NIC/NetInfoCollector.hpp"
#include "../FunctionExtension/GPU/GPUFunctions.h"
#include "../Execution/Event/ParameterizedEvent.hpp"
#include <list>

using namespace  std;
class ClusterServer
{
    static shared_ptr<NodesManager> nodesManager;
    static int heartbeatFreq;
    static bool useStorage;
    static shared_ptr<NetInfoCollector> netInfoCollector;
    static bool showInfos;
    static shared_ptr<RestfulClient> restfulClient;
    static shared_ptr<mutex> clientLock;
    static set<string> extensions;
    static atomic<bool> drainNode;

    static list<string> events;
    static shared_ptr<ParameterizedEvent> eventMonitor;

public:
    ClusterServer();

    static void start(shared_ptr<ParameterizedEvent> monitor);
    static shared_ptr<NodesManager> getNodesManager();

    static string getNodesInfo();
    static void resolveHeartbeat(string heartbeat);
    static void sendHeartbeat();
    static void heartbeatSender();
    static void startHeartbeat();

    static void nodeDraining(int nodeId);
    static void post_sync(string handle,string addrDest,vector<string> data);
    static string post_getResult_sync(string handle,string addrDest,vector<string> data);

    static shared_ptr<NetInfoCollector> getNetInfoCollector();

    static void checkExtensions();

    static void cleanTheNode();

    static set<string> getExtensionsAvailable()
    {
        return ClusterServer::nodesManager->getExtensionsAvailable();
    }

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
