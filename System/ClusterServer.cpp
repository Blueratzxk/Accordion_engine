//
// Created by zxk on 11/26/23.
//

#include "ClusterServer.h"
#include "../System/TaskServer.h"
#include "../Utils/Random.hpp"

void ClusterServer::start()
{
    nodesManager = make_shared<NodesManager>();
    nodesManager->initialNodes();

}

void ClusterServer::startHeartbeat(){
    thread(heartbeatSender).detach();
}
shared_ptr<NodesManager> ClusterServer::getNodesManager()
{
    while(nodesManager == NULL);
    return nodesManager;
}

void ClusterServer::resolveHeartbeat(std::string heartbeat) {

    nodesManager->resolveHeartbeat(Heartbeat::Deserialize(heartbeat));

}

void ClusterServer::sendHeartbeat() {
    string localIp = nodesManager->getLocalIp();
    string coordinatorIp = nodesManager->getCoordinatorAddr();



    Heartbeat heartbeat(localIp,TaskServer::getTaskServer()->getAllActiveTaskNums(),
                        TaskServer::getTaskServer()->getAllActiveThreadNums(),
                        TaskServer::getCpuInfoCollector()->getCpuCoreNums(),
                        TaskServer::getCpuInfoCollector()->getNodeCpuUsage(),
                        nodesManager->hasStorage(),ClusterServer::netInfoCollector->getReceivedRate(),
                        ClusterServer::netInfoCollector->getTransmittedRate(),
                        ClusterServer::netInfoCollector->getNICSpeed());
    http_Client::POST(coordinatorIp+"/v1/cluster/reportHeartbeat",{Heartbeat::Serialize(heartbeat)});

}

shared_ptr<NetInfoCollector> ClusterServer::getNetInfoCollector()
{
    if(ClusterServer::netInfoCollector == NULL)
    {
        WebConfig webConfig;
        ClusterServer::netInfoCollector = make_shared<NetInfoCollector>(webConfig.getNIC_Name());
    }
    return ClusterServer::netInfoCollector;
}

void ClusterServer::heartbeatSender() {


    WebConfig webConfig;

    while(true){

        if(ClusterServer::netInfoCollector == NULL)
            ClusterServer::netInfoCollector = make_shared<NetInfoCollector>(webConfig.getNIC_Name());

        int freq = RandomNumber::getInt(1000,1100);

        netInfoCollector->sampleAlpha();

        std::this_thread::sleep_for(std::chrono::milliseconds(freq));

        netInfoCollector->sampleBeta();
        netInfoCollector->computeRate(((double)freq/1000));

        sendHeartbeat();

        if(ClusterServer::showInfos)
            ClusterServer::getNodesManager()->displayAllNodesInfo();

    }
}

shared_ptr<NodesManager> ClusterServer::nodesManager = NULL;
int ClusterServer::heartbeatFreq = 2000;
shared_ptr<NetInfoCollector> ClusterServer::netInfoCollector = NULL;
bool ClusterServer::showInfos = false;