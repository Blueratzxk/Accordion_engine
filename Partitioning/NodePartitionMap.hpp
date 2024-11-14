//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_NODEPARTITIONMAP_HPP
#define OLVP_NODEPARTITIONMAP_HPP

#include <vector>
#include "../NodeCluster/Node.hpp"
using namespace std;

class NodePartitionMap
{
    vector<shared_ptr<ClusterNode>> partitionToNode;
    vector<vector<shared_ptr<ClusterNode>>> nodeGroups;
public:
    NodePartitionMap(vector<shared_ptr<ClusterNode>> partitionToNode)
    {
        this->partitionToNode = partitionToNode;
    }

    vector<shared_ptr<ClusterNode>> getPartitionToNode()
    {
        return this->partitionToNode;
    }
    vector<vector<shared_ptr<ClusterNode>>> getNodeGroups(){
        return this->nodeGroups;
    };

    void addNodeGroups(vector<vector<shared_ptr<ClusterNode>>> ngs)
    {
        this->nodeGroups = ngs;
    }

};


#endif //OLVP_NODEPARTITIONMAP_HPP
