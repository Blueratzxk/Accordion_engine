//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_NODESELECTOR_HPP
#define OLVP_NODESELECTOR_HPP


#include "../System/ClusterServer.h"
#include <random>
#include "spdlog/spdlog.h"

class NodeSelector
{

public:
    NodeSelector()
    {
    }
    shared_ptr<ClusterNode> getNodeByAddr(string addr)
    {
        vector<shared_ptr<ClusterNode>> nodes = ClusterServer::getNodesManager()->getAllNodes();
        for(auto node : nodes)
        {
            if(node->getNodeLocation() == addr) {
                if(ClusterServer::getNodesManager()->getOutputScheduleNodesLog())
                {
                    spdlog::info(node->getNodeLocation()+" is scheduled as table source!");
                }
                return node;
            }
        }
        spdlog::critical("Cannot find the node to schedule source! "+addr);
        return getNodesByMinThreadNums(1)[0];

    }

    vector<shared_ptr<ClusterNode>> getRandomNode()
    {
        vector<shared_ptr<ClusterNode>> nodes;
        int min = 0,max = ClusterServer::getNodesManager()->getNodesNums() - 1;
        if(max < 0)
        {
            cout << "No node can be used to schedule!"<<endl;

            return nodes;
        }
        random_device seed;//硬件生成随机数种子
        ranlux48 engine(seed());//利用种子生成随机数引擎
        uniform_int_distribution<> distrib(min, max);//设置随机数范围，并为均匀分布
        int random = distrib(engine);//随机数
        nodes.push_back(ClusterServer::getNodesManager()->getAllNodes()[random]);
        return nodes;
    }
    vector<shared_ptr<ClusterNode>> getRandomNodes(int num)
    {
        vector<shared_ptr<ClusterNode>> nodes;
        int min = 0,max = ClusterServer::getNodesManager()->getNodesNums() - 1;
        if(max < 0)
        {
            cout << "No node can be used to schedule!"<<endl;

            return nodes;
        }
        random_device seed;//硬件生成随机数种子
        ranlux48 engine(seed());//利用种子生成随机数引擎
        uniform_int_distribution<> distrib(min, max);//设置随机数范围，并为均匀分布
        for(int i = 0 ; i < num ; i++) {
            int random = distrib(engine);//随机数
            nodes.push_back(ClusterServer::getNodesManager()->getAllNodes()[random]);
        }
        return nodes;
    }

    shared_ptr<ClusterNode> getCoordinator()
    {
        return ClusterServer::getNodesManager()->getCoordinator();
    }
    shared_ptr<ClusterNode> findMinThreadNumNode(vector<pair<shared_ptr<ClusterNode>,int>> &vec)
    {
        if(vec.empty())
            return NULL;

        int min = INT_MAX;
        shared_ptr<ClusterNode> selectedNode = NULL;
        int selectedIndex = 0;
        for(int i = 0 ; i < vec.size() ; i++)
        {
            if(vec[i].second < min) {
                min = vec[i].second;
                selectedNode = vec[i].first;
                selectedIndex = i;
            }
        }



        vec[selectedIndex].second+=6;
        return selectedNode;

    }
    static bool cmp(pair<shared_ptr<ClusterNode>,int> a, pair<shared_ptr<ClusterNode>, int> b) {
        return a.second < b.second;
    }


    map<string,shared_ptr<ClusterNode>> filterStorageNodes(map<string,shared_ptr<ClusterNode>> all)
    {
        if(all.size() <= 1)
            return all;
        map<string,shared_ptr<ClusterNode>> nodes;
        for(auto node : all)
        {
            if(!node.second->ifHasStorage()) {
                nodes[node.first] = node.second;
            }
        }

        return nodes;
    }

    vector<shared_ptr<ClusterNode>> getNodesByMinThreadNums(int num)
    {

        map<string,shared_ptr<ClusterNode>> all = ClusterServer::getNodesManager()->getAllClusterNodes();

        if(!ClusterServer::getNodesManager()->ifUseStorageNode())
        {
            all = filterStorageNodes(all);
        }

        vector<shared_ptr<ClusterNode>> selectedNodes;

        vector<pair<shared_ptr<ClusterNode>,int>> vec;
        for(auto n : all)
        {
            vec.push_back(make_pair(n.second,n.second->getThreadNums()));
        }
        sort(vec.begin(),vec.end(),cmp);

        for(int i = 0 ; i < num ; i++)
        {
            shared_ptr<ClusterNode> selected = findMinThreadNumNode(vec);
            if(selected->is_Coordinator() && all.size() > 1)
                num++;
            else
                selectedNodes.push_back(selected);
        }

        for(auto item : selectedNodes)
        {
            ClusterServer::getNodesManager()->getAllClusterNodes()[item->getNodeLocation()]->addThreadNums(6);

        }

        if(ClusterServer::getNodesManager()->getOutputScheduleNodesLog())
        {
            for(auto node : selectedNodes)
            {
                spdlog::info(node->getNodeLocation()+" is scheduled as compuation node!");
            }
        }

        return selectedNodes;

    }


    vector<shared_ptr<ClusterNode>> getNodesByMinThreadNums(int mode,int num)
    {
        map<string,shared_ptr<ClusterNode>> all;
        if(mode == 1)
            all = ClusterServer::getNodesManager()->getInitialPlanClusterNodes();
        else
            all = ClusterServer::getNodesManager()->getAllClusterNodes();


        if(!ClusterServer::getNodesManager()->ifUseStorageNode())
        {
            all = filterStorageNodes(all);
        }

        vector<shared_ptr<ClusterNode>> selectedNodes;

        if(all.empty())
            return  selectedNodes;

        vector<pair<shared_ptr<ClusterNode>,int>> vec;
        for(auto n : all)
        {
            vec.push_back(make_pair(n.second,n.second->getThreadNums()));
        }
        sort(vec.begin(),vec.end(),cmp);

        for(int i = 0 ; i < num ; i++)
        {
            shared_ptr<ClusterNode> selected = findMinThreadNumNode(vec);
            if(selected->is_Coordinator() && all.size() > 1)
                num++;
            else
                selectedNodes.push_back(selected);
        }

        for(auto item : selectedNodes)
        {
            ClusterServer::getNodesManager()->getAllClusterNodes()[item->getNodeLocation()]->addThreadNums(6);

        }

        if(ClusterServer::getNodesManager()->getOutputScheduleNodesLog())
        {
            for(auto node : selectedNodes)
            {
                spdlog::info(node->getNodeLocation()+" is scheduled as compuation node! Mode is "+ to_string(mode));
            }
        }

        return selectedNodes;

    }

    vector<shared_ptr<ClusterNode>> getNodes(int num)
    {
        vector<shared_ptr<ClusterNode>> nodes;
        int min = 0,max = ClusterServer::getNodesManager()->getNodesNums() - 1;
        if(max < 0)
        {
            cout << "No node can be used to schedule!"<<endl;

            return nodes;
        }

        for(int i = 0 ; i < num ; i++)
            nodes.push_back(ClusterServer::getNodesManager()->getAllNodes()[i%ClusterServer::getNodesManager()->getNodesNums()]);


        return nodes;
    }

    vector<shared_ptr<ClusterNode>> getNodesReverse(int num)
    {
        vector<shared_ptr<ClusterNode>> nodes;
        int min = 0,max = ClusterServer::getNodesManager()->getNodesNums() - 1;
        if(max < 0)
        {
            cout << "No node can be used to schedule!"<<endl;

            return nodes;
        }

        for(int i = 0 ; i < num ; i++)
            nodes.push_back(ClusterServer::getNodesManager()->getAllNodes()[(i+max)%ClusterServer::getNodesManager()->getNodesNums()]);


        return nodes;
    }



};



#endif //OLVP_NODESELECTOR_HPP
