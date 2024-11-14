//
// Created by zxk on 6/11/23.
//

#ifndef OLVP_NODESMANAGER_H
#define OLVP_NODESMANAGER_H

#include "Node.hpp"

#include <iostream>
#include <fstream>
#include <vector>
#include "nlohmann/json.hpp"
#include "mutex"
#include "Heartbeat.hpp"
#include "../Config/WebConfig.hpp"
#include "spdlog/spdlog.h"
#include "../Config/NodeTypeConfig.hpp"
#include "../Config/ExecutionConfig.hpp"
using namespace  std;

class NodesManager
{
    vector<shared_ptr<ClusterNode>> allNodes;
    map<string,shared_ptr<ClusterNode>> allClusterNodes;

    map<string,shared_ptr<ClusterNode>> intialPlanNodes;

    mutex nodesLock;
    shared_ptr<ClusterNode> coordinator;
    WebConfig wconfig;

    bool heartbeatShow = false;
    NodeTypeConfig nodeTypeConfig;

    bool useStorageNode = true;

    bool outputScheduleNodesLog = false;

public:
    NodesManager();
    vector<shared_ptr<ClusterNode>> getAllNodes();
    int getNodesNums();
    void initialNodes();


    double getRemainingCpuUsagesImprovementRatio(double cpuUsage)
    {
        auto nodes = this->getAllComputeNodes();
        double allRatio = 0.0;
        for(auto node : nodes)
        {
            double ratio = 0;
            if(cpuUsage > 0)
                ratio = node.second->getRemainingCpuUsage()/cpuUsage;
            if(ratio > 0)
                allRatio+=ratio;
        }

        return allRatio;
    }

    shared_ptr<ClusterNode> getCoordinator()
    {
        return this->coordinator;
    }
    void resolveHeartbeat(Heartbeat heartbeat);

    void displayAllNodesInfo()
    {
        auto allNodesInfo = getAllClusterNodes();
        spdlog::info("------------------------------------------------");
        for(auto node : allNodesInfo)
        {
            node.second->display();
        }
        spdlog::info("------------------------------------------------");
    }

    bool getOutputScheduleNodesLog()
    {
        return  this->outputScheduleNodesLog;
    }
    void openOutputScheduleNodesLog()
    {
        this->outputScheduleNodesLog = true;
    }
    void closeOutputScheduleNodesLog()
    {
        this->outputScheduleNodesLog = false;
    }

    void showHeartbeat()
    {
        this->heartbeatShow = true;
    }
    void hideHeartbeat()
    {
        this->heartbeatShow = false;
    }

     bool ifUseStorageNode();

    map<string,shared_ptr<ClusterNode>> getAllClusterNodes()
    {
        nodesLock.lock();
        map<string,shared_ptr<ClusterNode>> all = this->allClusterNodes;

        nodesLock.unlock();
        return all;
    }
    map<string,shared_ptr<ClusterNode>> getAllComputeNodes()
    {
        map<string,shared_ptr<ClusterNode>> computeNodes;
        nodesLock.lock();
        map<string,shared_ptr<ClusterNode>> all = this->allClusterNodes;

        if(!ifUseStorageNode())
        {
            for(auto n : all)
            {
                if(!n.second->ifHasStorage() && !n.second->is_Coordinator())
                    computeNodes[n.first] = n.second;
            }
        }
        else
            computeNodes = all;
        if(all.size() == 1)
            computeNodes = all;

        nodesLock.unlock();
        return computeNodes;
    }
    map<string,shared_ptr<ClusterNode>> getInitialPlanClusterNodes()
    {
        map<string,shared_ptr<ClusterNode>> result;
        map<string,shared_ptr<ClusterNode>> allComputeNodes = this->getAllComputeNodes();
        nodesLock.lock();
     /*   if(!intialPlanNodes.empty()) {

            nodesLock.unlock();
            return this->intialPlanNodes;
        }
    */

        ExecutionConfig config;
        string nodes = config.getInitial_plan_used_nodes();

        if(!intialPlanNodes.empty() && atoi(nodes.c_str()) == this->intialPlanNodes.size()) {
            nodesLock.unlock();
            return this->intialPlanNodes;
        }


        if(allComputeNodes.size() < atoi(nodes.c_str()))
        {
            nodesLock.unlock();
            spdlog::critical("There is not enough compute nodes to build initial plan! Only has "+ to_string(allComputeNodes.size())+" but "+nodes +" need!");
            return result;
        }


        int index = 0;
        for(auto node : allComputeNodes)
        {
            result[node.first] = node.second;
            index ++;
            if(index == atoi(nodes.c_str()))
                break;
        }

        this->intialPlanNodes = result;

        nodesLock.unlock();

        return this->intialPlanNodes;
    }


    string getCoordinatorAddr(){
        return this->coordinator->getNodeLocation();
    }
    string getLocalIp()
    {
       return wconfig.getWebServerIp() + ":" + wconfig.getWebServerPort();
    }
    bool hasStorage()
    {
        return this->nodeTypeConfig.isStorage(wconfig.getWebServerIp() + ":" + wconfig.getWebServerPort());
    }

    bool hasNodes()
    {
        bool hasNodes;
        nodesLock.lock();
        this->allClusterNodes.size() > 0 ? hasNodes = true:hasNodes = false;
        nodesLock.unlock();
        return hasNodes;
    }

};

#endif //OLVP_NODESMANAGER_H
