//
// Created by zxk on 6/11/23.
//

#ifndef OLVP_SYSTEMPARTITIONINGHANDLE_H
#define OLVP_SYSTEMPARTITIONINGHANDLE_H






class DataPage;

#include "../Connector/ConnectorPartitioningHandle.hpp"

#include "PartitioningHandle.hpp"
#include "../NodeCluster/NodeSelector.hpp"
#include "NodePartitionMap.hpp"

#include "../common.h"

class bucketFunction
{
    virtual int getBucket(std::shared_ptr<DataPage> page ,int position) = 0;
};







class SystemPartitioningHandle:public ConnectorPartitioningHandle
{
public:
    enum SystemPartitioningType
    {
        SINGLE,
        FIXED,
        SOURCE,
        SCALED,
        HASH_SCALED,
        COORDINATOR_ONLY,
        ARBITRARY,
        SHUFFLE

    };

    enum SystemPartitionFunctionType {
        SINGLE_FUNC,
        HASH_FUNC,
        BROADCAST_FUNC,
        ROUND_ROBIN_FUNC,
        UNKNOWN_FUNC
    };


    SystemPartitioningType partitioningType;
    SystemPartitionFunctionType functionType;
    bool scalable = false;



public:



    static shared_ptr<PartitioningHandle> SINGLE_DISTRIBUTION;
    static shared_ptr<PartitioningHandle> COORDINATOR_DISTRIBUTION ;
    static shared_ptr<PartitioningHandle> FIXED_HASH_DISTRIBUTION ;
    static shared_ptr<PartitioningHandle> FIXED_ARBITRARY_DISTRIBUTION;
    static shared_ptr<PartitioningHandle> FIXED_BROADCAST_DISTRIBUTION;
    static shared_ptr<PartitioningHandle> SCALED_WRITER_DISTRIBUTION ;
    static shared_ptr<PartitioningHandle> SOURCE_DISTRIBUTION ;
    static shared_ptr<PartitioningHandle> ARBITRARY_DISTRIBUTION;
    static shared_ptr<PartitioningHandle> FIXED_PASSTHROUGH_DISTRIBUTION;

    static shared_ptr<PartitioningHandle> SCALED_SIMPLE_DISTRIBUTION_BUF;
    static shared_ptr<PartitioningHandle> SCALED_HASH_DISTRIBUTION_BUF;
    static shared_ptr<PartitioningHandle> SCALED_HASH_REDISTRIBUTION_BUF;

    static shared_ptr<PartitioningHandle> SCALED_HASH_SHUFFLE_STAGE_BUF;
    static shared_ptr<PartitioningHandle> SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF;

    static shared_ptr<list<std::shared_ptr<SystemPartitioningHandle>>> sysPartitioningHandles;

    static void handlesInit();

    string Serialize();

    std::shared_ptr<ConnectorPartitioningHandle> Deserialize(string handle);

    static shared_ptr<PartitioningHandle> createSystemPartitioning(SystemPartitioningType partitioningType,SystemPartitionFunctionType functionType,bool scalable);

    SystemPartitioningHandle();

    SystemPartitioningHandle(SystemPartitioningType partitioningType,SystemPartitionFunctionType functionType,bool scalable);

    NodePartitionMap getNodePartitionMap(NodeSelector selector);
    int getInitialPartitionCount();

    void getPartitionFunction();

    SystemPartitioningType getPartitioningType();
    SystemPartitionFunctionType getPartitioningFuncType();
    bool isScalable();
    bool isSingleNode();

    bool isCoordinatorOnly();

    static shared_ptr<PartitioningHandle> get(string handle);


    //-------------------------------------------------------------------------------------------------//

    class SingleBucketFunction:public bucketFunction
    {
        SingleBucketFunction(){}
        int getBucket(std::shared_ptr<DataPage> page, int position)
        {
            return 0;
        }
    };
    class RoundRobinBucketFunction:public bucketFunction
    {
        int bucketCount;
        int counter = 0;
    public:
        RoundRobinBucketFunction(int bucketCount)
        {
            this->bucketCount = bucketCount;
        }
        int getBucket(std::shared_ptr<DataPage> page, int position)
        {
            int bucket = counter % bucketCount;
            counter++;
            if(counter > bucketCount)
                counter = 0;
            return bucket;
        }
    };

    class HashBucketFunction:public bucketFunction {
        int bucketCount;
    public:
        HashBucketFunction(int bucketCount) {

            this->bucketCount = bucketCount;
        }

        int getBucket(std::shared_ptr<DataPage> page, int position) {
            return 0;
        }
    };
};







#endif //OLVP_SYSTEMPARTITIONINGHANDLE_H
