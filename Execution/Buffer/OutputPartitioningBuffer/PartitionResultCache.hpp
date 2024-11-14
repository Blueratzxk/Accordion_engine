//
// Created by zxk on 9/18/23.
//

#ifndef OLVP_PARTITIONRESULTCACHE_HPP
#define OLVP_PARTITIONRESULTCACHE_HPP

#define pCache shared_ptr<vector<vector<shared_ptr<DataPage>>>>

class PartitionResultCache
{
    mutex lock;
    map<int,pCache> partitionsCache;
    map<int,int> partitionCountToGroupId;
    vector<int> currentBufferSlice;
    int curGroupId;

public:
    PartitionResultCache(){

    }

    pCache regPartitionCache(int partitionCount,int groupId,vector<int> bufferIds)
    {
        this->currentBufferSlice = bufferIds;
        pCache p = NULL;
        lock.lock();
        if(partitionsCache.count(partitionCount) == 0) {
            p = make_shared<vector<vector<shared_ptr<DataPage>>>>(partitionCount);
            partitionsCache[partitionCount] = p;
            partitionCountToGroupId[partitionCount] = groupId;
        }
        lock.unlock();

        this->curGroupId = partitionCountToGroupId[partitionCount];

        return p;

    }

    vector<int> getBufferSlice()
    {
        return this->currentBufferSlice;
    }

    int getCurGroupId()
    {
        return this->curGroupId;
    }

    bool partitionCacheExist(int partitionCount)
    {
        bool isExist = false;
        lock.lock();
        if(this->partitionsCache.count(partitionCount) > 0)
            isExist = true;
        lock.unlock();
        return isExist;
    }

    vector<shared_ptr<DataPage>> getPartition(int partitionCount,int partitionNumber)
    {
        lock.lock();
        pCache p = partitionsCache[partitionCount];
        return (*p)[partitionNumber];
        lock.unlock();
    }






};


#endif //OLVP_PARTITIONRESULTCACHE_HPP
