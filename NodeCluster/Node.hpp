//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_NODE_HPP
#define OLVP_NODE_HPP

#include <string>
using namespace std;
#include <atomic>
#include <spdlog/spdlog.h>
#include "../Utils/TextColor.h"
#include <set>

class ClusterNode
{
public:
    enum NodeStatus
    {
        ALIVE,
        DEAD,
        DRAIN
    };

private:
    string nodeIdentifier;

    string httpUrl;

    bool isCoordinator = false;
    atomic<NodeStatus> nodeStatus;
    atomic<int> activeTaskNums = 0;
    atomic<int> activeThreadNums = 0;
    atomic<float> cpuUsage = 0.0;
    atomic<int> coreNums = 0;

    atomic<double> netRecRate = 0.0;
    atomic<double> netTransRate = 0.0;
    int netSpeed = 0;
    bool hasStorage = false;

    set<string> extensions;

    int nodeId;

public:
    ClusterNode(int nodeId,string identifier,string httpUrl,bool hasStorage){
        this->nodeIdentifier = identifier;
        this->httpUrl = httpUrl;
        this->hasStorage = hasStorage;
        this->nodeId = nodeId;
        this->nodeStatus = ALIVE;
    }

    int getNodeId()
    {
        return this->nodeId;
    }
    string getNodeLocation()
    {
        return httpUrl;
    }

    float getCurrentCpuUsage()
    {
        return this->cpuUsage;
    }

    void drainNode()
    {
        if(!is_Coordinator())
            this->nodeStatus = DRAIN;
    }

    void transitionToAlive()
    {
        this->nodeStatus = ALIVE;
    }

    NodeStatus status()
    {
        return this->nodeStatus;
    }

    string getNodeStatus(){
        if(this->nodeStatus == ALIVE)
            return "ALIVE";
        else if(this->nodeStatus == DEAD)
            return "DEAD";
        else if(this->nodeStatus == DRAIN)
            return "DRAIN";
        else
            return "Unknown";
    }

    int getCoreNums()
    {
        return this->coreNums;
    }

    void setCoordinator()
    {
        this->isCoordinator = true;
    }

    void updateExtensions(set<string>extensions)
    {
        this->extensions = extensions;
    }

    set<string> getExtensions()
    {
        return this->extensions;
    }

    bool is_Coordinator()
    {
        return this->isCoordinator;
    }
    void updateTaskNums(int num)
    {
        this->activeTaskNums = num;
    }
    void updateCoreNums(int num)
    {
        this->coreNums = num;
    }
    void updateCpuUsage(float num)
    {
        this->cpuUsage = num;
    }

    void updateNetRecRate(double val){this->netRecRate = val;}
    void updateNetTransRate(double val) {this->netTransRate = val;}

    void setNetSpeed(int speed)
    {
        this->netSpeed = speed;
    }
    int getNetSpeed()
    {
        return this->netSpeed;
    }

    float getMaxNetSpeed()
    {
        return ((float)this->netSpeed)*1.024/8*1000;
    }
    float difference()
    {
        return 20000;
    }
    bool hitTransBottleNeck()
    {
        if(this->getMaxNetSpeed()-this->getCurrentNetTransRate() < this->difference())
            return true;
        else
            return false;
    }
    bool hitRecBottleNeck()
    {
        if(this->getMaxNetSpeed()-this->getCurrentNetRecRate() < this->difference())
            return true;
        else
            return false;
    }

    double getRemainingTransThroughput()
    {
        auto re = this->getMaxNetSpeed()-this->getCurrentNetTransRate();
        if(re < 0)
            return 0;
        return re;
    }
    double getRemainingRecThroughput()
    {
        auto re =  this->getMaxNetSpeed()-this->getCurrentNetRecRate();
        if(re < 0)
            return 0;
        return re;
    }

    double getCurrentNetRecRate(){return this->netRecRate;}
    double getCurrentNetTransRate() {return this->netTransRate;}

    void addThreadNums(int num)
    {
        this->activeThreadNums += num;
    }

    void updateThreadNums(int num)
    {
        this->activeThreadNums = num;
    }

    int getThreadNums()
    {
        return this->activeThreadNums;
    }
    bool ifHasStorage()
    {
        return this->hasStorage;
    }

    double getRemainingCpuUsage()
    {
        return this->coreNums*100.0 - (double)this->cpuUsage;
    }
    void display()
    {
        string re;
        re.append("id:");
        re.append(TextColor::numberTextTrim(to_string(this->nodeId),2," "));
        re.append("|");
        re.append("ip:");
        re.append(httpUrl);
        if(this->isCoordinator) {
            re.append("|");
            re.append(TextColor::LIGHT_PURPLE("Coordinator"));
        }
        else {
            re.append("|");
            re.append(TextColor::LIGHT_CYAN("  Worker   "));
        }
        if(this->hasStorage){
            re.append("|");
            re.append("node_type:");
            re.append(TextColor::YELLOW("storage"));
        }
        else if(!this->isCoordinator){
            re.append("|");
            re.append("node_type:");
            re.append(TextColor::LIGHT_GREEN("compute"));
        }
        else
        {
            re.append("|");
            re.append("node_type:");
            re.append(TextColor::LIGHT_PURPLE(" coor  "));
        }


        re.append("|");
        re.append("cpu usage:");
        re.append(TextColor::LIGHT_RED(TextColor::numberTextTrim(to_string(this->cpuUsage),6,"0")));
        re.append("|");
        re.append("cpu core nums:");
        re.append(TextColor::numberTextTrim(to_string(this->coreNums),2," "));
        re.append("|");
        re.append("thread nums:");
        re.append(TextColor::numberTextTrim(to_string(this->activeThreadNums),2," "));
        re.append("|");
        re.append("task nums:");
        re.append(TextColor::numberTextTrim(to_string(this->activeTaskNums),2," "));
        re.append("|");
        re.append("NIC receive rate:");
        re.append(TextColor::numberTextTrim(to_string(this->netRecRate),6,"0"));
        re.append("|");
        re.append("NIC trans rate:");
        re.append(TextColor::LIGHT_RED(TextColor::numberTextTrim(to_string(this->netTransRate),6,"0")));


        re.append("|");
        string exts;
        int round = 0;
        for(auto ext : extensions) {
            if(round > 0)
                exts.append(ext).append(" ");
            else
                exts.append(ext);
            round++;
        }
        if(!exts.empty()) {
            re.append("Exts:");
            re.append(TextColor::LIGHT_GREEN(exts));
        }
        else
        {
            re.append("Exts:");
            re.append("NO ");
        }




        re.append("|");
        spdlog::info(re);
    }

    string getNodeType()
    {
        if(this->isCoordinator)
            return "Coordinator";
        else if(this->hasStorage)
            return "Storage";
        else
            return "Compute";
    }

    bool operator<(const ClusterNode& g)const {
        if (nodeIdentifier < g.nodeIdentifier) {
            return true;
        }
        return false;
    }


};


#endif //OLVP_NODE_HPP
