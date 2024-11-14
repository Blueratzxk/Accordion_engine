//
// Created by zxk on 6/11/23.
//


#include "NodesManager.h"
#include "../Config/ExecutionConfig.hpp"

NodesManager::NodesManager(){

}


vector<shared_ptr<ClusterNode>> NodesManager::getAllNodes() {

    nodesLock.lock();

    vector<shared_ptr<ClusterNode>> nodes;

    for(auto node : this->allClusterNodes)
    {
        nodes.push_back(node.second);
    }

    nodesLock.unlock();




    return nodes;
}
int NodesManager::getNodesNums() {

    nodesLock.lock();
    int nums = this->allClusterNodes.size();
    nodesLock.unlock();
    return nums;
}

bool NodesManager::ifUseStorageNode()
{

    return this->useStorageNode;
}

void NodesManager::initialNodes() {

    nodesLock.lock();
    vector<shared_ptr<ClusterNode>> nodes;




    string Restful_Web_Server_IP = wconfig.getCoordinatorWebServerIp();
    string Restful_Web_Server_Port = wconfig.getCoordinatorWebServerPort();
    string Arrow_RPC_Server_IP = wconfig.getCoordinatorRPCServerIp();
    string Arrow_RPC_Server_Port = wconfig.getCoordinatorRPCServerPort();

    shared_ptr<ClusterNode> node = make_shared<ClusterNode>("coordinator",
                                                            Restful_Web_Server_IP + ":" + Restful_Web_Server_Port,
                                                            false);
    coordinator = node;


    nodesLock.unlock();


    ExecutionConfig config;
    this->useStorageNode = config.ifDoNotUseStorageNode() == "true" ? false : true;
}

 void NodesManager::resolveHeartbeat(Heartbeat heartbeat){

    nodesLock.lock();
    if(this->allClusterNodes.count(heartbeat.getNodeHttpURL()) == 0) {
        this->allClusterNodes[heartbeat.getNodeHttpURL()] = make_shared<ClusterNode>(heartbeat.getNodeHttpURL(),
                                                                                     heartbeat.getNodeHttpURL(),heartbeat.ifHasStorgae());
        spdlog::info("New worker joined! Ip => ["+heartbeat.getNodeHttpURL()+"]");

        if(allClusterNodes[heartbeat.getNodeHttpURL()]->getNodeLocation() == getCoordinator()->getNodeLocation())
        {
            allClusterNodes[heartbeat.getNodeHttpURL()]->setCoordinator();
        }

        this->allClusterNodes[heartbeat.getNodeHttpURL()]->setNetSpeed(heartbeat.getNet_speed());

    }
    else
    {
        this->allClusterNodes[heartbeat.getNodeHttpURL()]->updateTaskNums(heartbeat.getActiveTaskNums());
        this->allClusterNodes[heartbeat.getNodeHttpURL()]->updateThreadNums(heartbeat.getActiveThreadNums());
        this->allClusterNodes[heartbeat.getNodeHttpURL()]->updateCoreNums(heartbeat.getNodeCpuCoreNums());
        this->allClusterNodes[heartbeat.getNodeHttpURL()]->updateCpuUsage(heartbeat.getNodeCpuUsage());
        this->allClusterNodes[heartbeat.getNodeHttpURL()]->updateNetRecRate(heartbeat.getNet_receiveRate());
        this->allClusterNodes[heartbeat.getNodeHttpURL()]->updateNetTransRate(heartbeat.getNet_transRate());
    }


    if(this->heartbeatShow)
        spdlog::info("Heart Beat > " + heartbeat.getNodeHttpURL() +" threadNums:"+
    to_string(heartbeat.getActiveThreadNums()) + " taskNums:"+to_string(heartbeat.getActiveTaskNums())
                                                              + " hasStorage:"+(heartbeat.ifHasStorgae() == true?"true":"false"));

    nodesLock.unlock();
}


