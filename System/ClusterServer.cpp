//
// Created by zxk on 11/26/23.
//

#include "ClusterServer.h"
#include "../System/TaskServer.h"
#include "../Utils/Random.hpp"

void ClusterServer::start(shared_ptr<ParameterizedEvent> monitor)
{
    nodesManager = make_shared<NodesManager>();
    nodesManager->initialNodes();
    eventMonitor = monitor;
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

    string event = nodesManager->resolveHeartbeat(Heartbeat::Deserialize(heartbeat));
    if(event != "")
        eventMonitor->notify("cluster",event);
}

void ClusterServer::nodeDraining(int nodeId)
{
    auto nodeUrl = nodesManager->getNodeUrlByNodeId(nodeId);
    if(nodeUrl != "NULL")
        eventMonitor->notify("cluster",nodeUrl);
    else
        spdlog::info("Cannot find the node " + to_string(nodeId)+"!");
}

void ClusterServer::checkExtensions() {
    GPUFunctions gpuFunctions;
    if(gpuFunctions.extensionUsable())
        extensions.insert("GPU");
    else
        extensions.erase("GPU");

    ExecutionConfig executionConfig;
    if(executionConfig.getExtensionTest() == "true")
        extensions.insert("TEST");


}

void ClusterServer::sendHeartbeat() {
    string localIp = nodesManager->getLocalIp();
    string coordinatorIp = nodesManager->getCoordinatorAddr();
    checkExtensions();

    Heartbeat heartbeat(localIp,TaskServer::getTaskServer()->getAllActiveTaskNums(),
                        TaskServer::getTaskServer()->getAllActiveThreadNums(),
                        TaskServer::getCpuInfoCollector()->getCpuCoreNums(),
                        TaskServer::getCpuInfoCollector()->getNodeCpuUsage(),
                        nodesManager->hasStorage(),ClusterServer::netInfoCollector->getReceivedRate(),
                        ClusterServer::netInfoCollector->getTransmittedRate(),
                        ClusterServer::netInfoCollector->getNICSpeed(),
                        extensions,ClusterServer::drainNode);
    if(ClusterServer::drainNode)
        ClusterServer::drainNode = false;

    ClusterServer::post_getResult_sync(coordinatorIp,coordinatorIp+"/v1/cluster/reportHeartbeat",{Heartbeat::Serialize(heartbeat)});

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

void ClusterServer::cleanTheNode()
{
    ClusterServer::drainNode = true;
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
void ClusterServer::post_sync(string handle, string addrDest, vector<string> data){

    ClusterServer::restfulClient->POST_Sync(handle,addrDest,data);
}
string ClusterServer::post_getResult_sync(string handle, string addrDest, vector<string> data) {

    return ClusterServer::restfulClient->POST_GetResult_Sync(handle,addrDest,data);
}

shared_ptr<NodesManager> ClusterServer::nodesManager = NULL;
int ClusterServer::heartbeatFreq = 2000;
shared_ptr<NetInfoCollector> ClusterServer::netInfoCollector = NULL;
bool ClusterServer::showInfos = false;
atomic<bool> ClusterServer::drainNode = false;
shared_ptr<RestfulClient> ClusterServer::restfulClient = make_shared<RestfulClient>();
shared_ptr<mutex> ClusterServer::clientLock = make_shared<mutex>();
set<string> ClusterServer::extensions = {};
list<string> ClusterServer::events = {};
shared_ptr<ParameterizedEvent> ClusterServer::eventMonitor = NULL;