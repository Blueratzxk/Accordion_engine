//
// Created by zxk on 6/11/23.
//



#include "SystemPartitioningHandle.h"
#include "../Config/ExecutionConfig.hpp"




string SystemPartitioningHandle::Serialize() {
    nlohmann::json systemPartitioning;
    systemPartitioning["partitioningType"] = this->getPartitioningType();
    systemPartitioning["functionType"] = this->getPartitioningFuncType();
    systemPartitioning["scalable"] = this->scalable;
    string result = systemPartitioning.dump();
    return result;
}

std::shared_ptr<ConnectorPartitioningHandle> SystemPartitioningHandle::Deserialize(string handle) {
    nlohmann::json sys = nlohmann::json::parse(handle);
    SystemPartitioningHandle sysHandle(sys["partitioningType"], sys["functionType"],sys["scalable"]);

    SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION");

    for (auto sHandle: *sysPartitioningHandles) {
        if (sHandle->partitioningType == sysHandle.partitioningType &&
            sHandle->functionType == sysHandle.functionType) {
            return sHandle;
        }
    }

    spdlog::critical("deserialize cannot find partitioning Handle!");
    exit(0);
}

shared_ptr<PartitioningHandle> SystemPartitioningHandle::createSystemPartitioning(SystemPartitioningType partitioningType,SystemPartitionFunctionType functionType,bool scalable) {
    std::shared_ptr<SystemPartitioningHandle> handle = std::make_shared<SystemPartitioningHandle>(partitioningType,
                                                                                                  functionType,scalable);
    sysPartitioningHandles->push_back(handle);
    return make_shared<PartitioningHandle>(handle);
}

SystemPartitioningHandle::SystemPartitioningHandle():ConnectorPartitioningHandle("SystemPartitioningHandle"){}

SystemPartitioningHandle::SystemPartitioningHandle(SystemPartitioningType partitioningType,SystemPartitionFunctionType functionType,bool scalable):ConnectorPartitioningHandle("SystemPartitioningHandle") {
    this->partitioningType = partitioningType;
    this->functionType = functionType;
    this->scalable = scalable;
}


NodePartitionMap SystemPartitioningHandle::getNodePartitionMap(NodeSelector selector) {
    vector<shared_ptr<ClusterNode>> nodes;
    vector<vector<shared_ptr<ClusterNode>>> tgroups;

    int INITIAL_PLAN_NODES_MODE = 0;
    ExecutionConfig configs;
    string mode = configs.getInitial_plan_used_nodes();
    if(mode != "-1")
        INITIAL_PLAN_NODES_MODE = 1;


    if (this->partitioningType == SystemPartitioningHandle::COORDINATOR_ONLY) {
        nodes = selector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,1);
    } else if (this->partitioningType == SystemPartitioningHandle::SINGLE) {
        nodes = selector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,1);
    } else if (this->partitioningType == SystemPartitioningHandle::FIXED || this->partitioningType == SystemPartitioningHandle::SCALED) {
        ExecutionConfig config;
        int stageConcur = atoi(config.getInitial_intra_stage_concurrency().c_str());
        nodes = selector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,stageConcur);//this is a config-value,this should be configured in somewhere

        int tgCount = atoi(config.getInitial_task_group_concurrency().c_str());
        for(int i = 0 ; i < tgCount - 1; i++)
        {
            tgroups.push_back(selector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,stageConcur));
        }

    }
    else if (this->partitioningType == SystemPartitioningHandle::HASH_SCALED) {
        ExecutionConfig config;
        int concur = atoi(config.getInitial_hash_partition_concurrency().c_str());
        nodes = selector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,concur);//this is a config-value,this should be configured in somewhere
    }
    else if (this->partitioningType == SystemPartitioningHandle::SHUFFLE) {
        ExecutionConfig config;
        int concur = atoi(config.getInitial_intra_stage_concurrency().c_str());
        nodes = selector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,concur);//this is a config-value,this should be configured in somewhere
    }
    else {
        nodes = selector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,1);
    }
    NodePartitionMap np(nodes) ;
    np.addNodeGroups(tgroups);

    return np;
}

int SystemPartitioningHandle::getInitialPartitionCount() {
    if (this->partitioningType == SystemPartitioningHandle::COORDINATOR_ONLY) {
        return 1;
    } else if (this->partitioningType == SystemPartitioningHandle::SINGLE) {
        return 1;
    } else if (this->partitioningType == SystemPartitioningHandle::FIXED) {
        return 1;
    } else {
        return 1;
    }
}


void SystemPartitioningHandle::getPartitionFunction() {

}

SystemPartitioningHandle::SystemPartitioningType SystemPartitioningHandle::getPartitioningType() {
    return this->partitioningType;
}
SystemPartitioningHandle::SystemPartitionFunctionType SystemPartitioningHandle::getPartitioningFuncType() {
    return this->functionType;
}

bool SystemPartitioningHandle::isScalable() {
    return this->scalable;
}
bool SystemPartitioningHandle::isSingleNode() {
    return this->partitioningType == SystemPartitioningType::COORDINATOR_ONLY ||
           this->partitioningType == SystemPartitioningType::SINGLE;
}


bool SystemPartitioningHandle::isCoordinatorOnly() {
    return this->partitioningType == SystemPartitioningType::COORDINATOR_ONLY;
}


void  initHandles()
{
    if(SystemPartitioningHandle::SINGLE_DISTRIBUTION == NULL) {
        SystemPartitioningHandle::sysPartitioningHandles = make_shared<list<std::shared_ptr<SystemPartitioningHandle>>>();
        SystemPartitioningHandle::SINGLE_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::SINGLE, SystemPartitioningHandle::SINGLE_FUNC,false);
        SystemPartitioningHandle::COORDINATOR_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::COORDINATOR_ONLY, SystemPartitioningHandle::SINGLE_FUNC,false);
        SystemPartitioningHandle::FIXED_HASH_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::FIXED, SystemPartitioningHandle::HASH_FUNC,true);
        SystemPartitioningHandle::FIXED_ARBITRARY_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::FIXED, SystemPartitioningHandle::ROUND_ROBIN_FUNC,false);
        SystemPartitioningHandle::FIXED_BROADCAST_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::FIXED, SystemPartitioningHandle::BROADCAST_FUNC,true);
        SystemPartitioningHandle::SCALED_WRITER_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::SCALED, SystemPartitioningHandle::ROUND_ROBIN_FUNC,false);
        SystemPartitioningHandle::SOURCE_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::SOURCE, SystemPartitioningHandle::UNKNOWN_FUNC,false);
        SystemPartitioningHandle::ARBITRARY_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::ARBITRARY, SystemPartitioningHandle::UNKNOWN_FUNC,false);
        SystemPartitioningHandle::FIXED_PASSTHROUGH_DISTRIBUTION = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::FIXED, SystemPartitioningHandle::UNKNOWN_FUNC,false);
        SystemPartitioningHandle::SCALED_SIMPLE_DISTRIBUTION_BUF = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::SCALED, SystemPartitioningHandle::UNKNOWN_FUNC,true);
        SystemPartitioningHandle::SCALED_HASH_DISTRIBUTION_BUF = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::HASH_SCALED, SystemPartitioningHandle::HASH_FUNC,true);
        SystemPartitioningHandle::SCALED_HASH_REDISTRIBUTION_BUF = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::HASH_SCALED, SystemPartitioningHandle::HASH_FUNC,true);
        SystemPartitioningHandle::SCALED_HASH_SHUFFLE_STAGE_BUF = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::SHUFFLE, SystemPartitioningHandle::HASH_FUNC,true);
        SystemPartitioningHandle::SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF = SystemPartitioningHandle::createSystemPartitioning(
                SystemPartitioningHandle::SHUFFLE, SystemPartitioningHandle::UNKNOWN_FUNC,true);



    }


}

shared_ptr<PartitioningHandle> SystemPartitioningHandle::get(std::string handle) {


    initHandles();
    if(handle == "SINGLE_DISTRIBUTION")
        return SystemPartitioningHandle::SINGLE_DISTRIBUTION;
    if(handle == "COORDINATOR_DISTRIBUTION")
        return SystemPartitioningHandle::COORDINATOR_DISTRIBUTION;
    if(handle == "FIXED_HASH_DISTRIBUTION")
        return SystemPartitioningHandle::FIXED_HASH_DISTRIBUTION;
    if(handle == "FIXED_ARBITRARY_DISTRIBUTION")
        return SystemPartitioningHandle::FIXED_ARBITRARY_DISTRIBUTION;
    if(handle == "FIXED_BROADCAST_DISTRIBUTION")
        return SystemPartitioningHandle::FIXED_BROADCAST_DISTRIBUTION;
    if(handle == "SCALED_WRITER_DISTRIBUTION")
        return SystemPartitioningHandle::SCALED_WRITER_DISTRIBUTION;
    if(handle == "SOURCE_DISTRIBUTION")
        return SystemPartitioningHandle::SOURCE_DISTRIBUTION;
    if(handle == "ARBITRARY_DISTRIBUTION")
        return SystemPartitioningHandle::ARBITRARY_DISTRIBUTION;
    if(handle == "FIXED_PASSTHROUGH_DISTRIBUTION")
        return SystemPartitioningHandle::FIXED_PASSTHROUGH_DISTRIBUTION;
    if(handle == "SCALED_SIMPLE_DISTRIBUTION_BUF")
        return SystemPartitioningHandle::SCALED_SIMPLE_DISTRIBUTION_BUF;
    if(handle == "SCALED_HASH_DISTRIBUTION_BUF")
        return SystemPartitioningHandle::SCALED_HASH_DISTRIBUTION_BUF;
    if(handle == "SCALED_HASH_REDISTRIBUTION_BUF")
        return SystemPartitioningHandle::SCALED_HASH_REDISTRIBUTION_BUF;
    if(handle == "SCALED_HASH_SHUFFLE_STAGE_BUF")
        return SystemPartitioningHandle::SCALED_HASH_SHUFFLE_STAGE_BUF;
    if(handle == "SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF")
        return SystemPartitioningHandle::SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF;



    return NULL;


}


shared_ptr<list<std::shared_ptr<SystemPartitioningHandle>>> SystemPartitioningHandle::sysPartitioningHandles = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SINGLE_DISTRIBUTION = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::COORDINATOR_DISTRIBUTION  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::FIXED_HASH_DISTRIBUTION  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::FIXED_ARBITRARY_DISTRIBUTION  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::FIXED_BROADCAST_DISTRIBUTION = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SCALED_WRITER_DISTRIBUTION  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SOURCE_DISTRIBUTION = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::ARBITRARY_DISTRIBUTION  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::FIXED_PASSTHROUGH_DISTRIBUTION  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SCALED_SIMPLE_DISTRIBUTION_BUF = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SCALED_HASH_DISTRIBUTION_BUF  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SCALED_HASH_REDISTRIBUTION_BUF  = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SCALED_HASH_SHUFFLE_STAGE_BUF = NULL;
shared_ptr<PartitioningHandle> SystemPartitioningHandle::SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF = NULL;