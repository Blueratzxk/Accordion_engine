//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_TASKBUFFERGROUP_HPP
#define OLVP_TASKBUFFERGROUP_HPP


#include "PartitionCount_BufferMap.hpp"



class TaskBufferGroup
{
    atomic<int> maxPartitionNumber = 0;
    atomic<int> nextGroupId = 0;
    map<int,shared_ptr<PartitionCount_BuffersMap>> partitionCount_taskGroupMap;

    map<int,int> taskGroupPagesLimits;
    int taskGroupChangeSize = 1;

    shared_ptr<tbb::concurrent_map<string,shared_ptr<ClientBuffer>>> buffers;
    shared_ptr<TaskContext> taskContext;

    vector<int> hashColumns;

    atomic<int> nextSelectedGroupId = 0;
    atomic<int> preSelectedGroupId = 0;

    vector<shared_ptr<DataPage>> allPages;
    mutex groupLock;

    shared_ptr<mutex> buffersLock;

    atomic<bool> endPageFound = false;
    shared_ptr<DataPage> endPageAddr = NULL;

    OutputBufferSchema::PartitioningBufferType pType;

    shared_ptr<DataPage> tempEndPage;

    bool closePreTaskGroup = false;
    bool lazyClosePreTaskGroup = false;

    bool partitionsCacheMode = true;

    shared_ptr<PartitionResultCache> prc;
    map<int,int> bufferIdToPartitionCount;

    bool repeatable = false;

    int forceShuffleExecutorNum = -1;

    atomic<int> currentPages = 0;

    int unprocessLimit = 0;

    enum tuningRoles {None,consumer,producer};
    std::atomic<tuningRoles> curRole = None;

    mutex timelock;
    shared_ptr<std::chrono::system_clock::time_point> start = NULL;

    int bufferTuneCircle = 500; //ms
    map<int,shared_ptr<PartitionCount_BuffersMap>> bufferIdToGroupId;


public:
    TaskBufferGroup(){


        tempEndPage = DataPage::getEndPage();
        this->buffers = make_shared<tbb::concurrent_map<string,shared_ptr<ClientBuffer>>>();
        this->buffersLock = make_shared<mutex>();
        this->prc = make_shared<PartitionResultCache>();

        ExecutionConfig executionConfig;
        string mode = executionConfig.getTask_group_close_mode();
        if(mode == "true")
            this->closePreTaskGroup = true;
        else
            this->closePreTaskGroup = false;

        string mode2 = executionConfig.getLazy_task_group_close_mode();
        if(mode2 == "true")
            this->lazyClosePreTaskGroup = true;
        else
            this->lazyClosePreTaskGroup = false;

    }

    void addTaskContext(shared_ptr<TaskContext> taskContext)
    {
        this->taskContext = taskContext;
        groupLock.lock();

        for(auto pt : this->partitionCount_taskGroupMap)
        {
            pt.second->addTaskContext(this->taskContext);
        }
        groupLock.unlock();
    }


    map<int,int> getTaskGroupMap()
    {
        map<int,int> tgMap;

        for(auto pt : this->partitionCount_taskGroupMap)
        {
            tgMap[pt.first] = pt.second->getPartitionCount();
        }
        return tgMap;

    }

    void setForceShuffleExecutorNum(int num)
    {
        this->forceShuffleExecutorNum = num;
    }

    void reportDownStreamTaskBuildCompletedInfo(string bufferId)
    {
        shared_ptr<PartitionCount_BuffersMap> group = NULL;

        int groupId = -1;
        groupLock.lock();



        for(auto pt : this->partitionCount_taskGroupMap)
        {
            if(pt.second->isBufferInGroup(atoi(bufferId.c_str())))
            {
                groupId = pt.first;
                group = pt.second;
                break;
            }
        }
        groupLock.unlock();
        if(group == NULL)
            spdlog::error("partitionTaskGroup ERROR ! Cannot identify this bufferId ["+bufferId+"]");



        if(this->closePreTaskGroup && this->lazyClosePreTaskGroup) {
            if (group->recordBuildCount()) {
                spdlog::debug("Lazy close pre task group!");

                groupLock.lock();
                this->closeGroupInOnceType(groupId - 1);
                groupLock.unlock();
            }
        }

    }

    void setBuffers(vector<int> hashColumnsIn,OutputBufferSchema::PartitioningBufferType type)
    {
        this->hashColumns = hashColumnsIn;
        this->pType = type;

        if (this->pType == OutputBufferSchema::PAR_REPEATABLE) {
            this->repeatable = true;
        }
    }
    void addPartitionTaskGroup(int partitionCount)
    {

        vector<int> bufferIdSlice;

        buffersLock->lock();
        for(int i = 0 ; i < partitionCount ; i++)
        {
            int bufferId = maxPartitionNumber+i;
            if((buffers)->count(to_string(bufferId)) == 0) {
                (*buffers)[to_string(bufferId)] = make_shared<ClientBuffer>(to_string(bufferId));
            }
            bufferIdToPartitionCount[bufferId] = partitionCount;
            bufferIdSlice.push_back(bufferId);
        }
        buffersLock->unlock();



        groupLock.lock();
        if(!this->partitionsCacheMode || !this->repeatable) {
            this->partitionCount_taskGroupMap[this->nextGroupId] = make_shared<PartitionCount_BuffersMap>(
                    this->taskContext,
                    this->forceShuffleExecutorNum,
                    maxPartitionNumber,
                    partitionCount,
                    this->buffers,
                    this->buffersLock,
                    this->hashColumns,
                    this->repeatable);

            this->taskGroupPagesLimits[this->nextGroupId] = 1;
        }
        else if(this->repeatable)
        {
            auto ppcache = this->prc->regPartitionCache(partitionCount,this->nextGroupId,bufferIdSlice);
            this->partitionCount_taskGroupMap[this->nextGroupId] = make_shared<PartitionCount_BuffersMap>(
                    this->taskContext,
                    this->forceShuffleExecutorNum,
                    maxPartitionNumber,
                    partitionCount,
                    this->buffers,
                    this->buffersLock,
                    this->hashColumns,
                    this->repeatable,
                    ppcache);

            this->taskGroupPagesLimits[this->nextGroupId] = 1000;

            //It means that this partitionCount exists,and we can use bufferSlice and bufferIds to delegate.
            if(ppcache == NULL)
            {
                vector<int> bufferIds = this->prc->getBufferSlice();
                int groupId = this->prc->getCurGroupId();

                //tuo guan ...
                this->partitionCount_taskGroupMap[groupId]->addDelegateBuffer(bufferIds);

            }
        }
        else
        {
            if(!this->partitionsCacheMode || !this->repeatable) {
                this->partitionCount_taskGroupMap[this->nextGroupId] = make_shared<PartitionCount_BuffersMap>(
                        this->taskContext,
                        this->forceShuffleExecutorNum,
                        maxPartitionNumber,
                        partitionCount,
                        this->buffers,
                        this->buffersLock,
                        this->hashColumns,
                        this->repeatable);

                this->taskGroupPagesLimits[this->nextGroupId] = 1;
            }
        }


        auto groupMap = this->partitionCount_taskGroupMap;


        if(this->pType == OutputBufferSchema::PAR_REPEATABLE)
        {

            if(this->endPageFound)
                groupMap[this->nextGroupId]->enqueuePages({this->allPages});

        }
        else
        {
            if(this->nextGroupId > 0 && this->closePreTaskGroup && !this->lazyClosePreTaskGroup)
            {

                spdlog::debug("Direct close pre task group!");
                this->closeGroupInOnceType(this->nextGroupId - 1);

            }

            if(this->endPageFound)
                groupMap[this->nextGroupId]->enqueuePages({this->endPageAddr});

        }

        for(auto bid : bufferIdSlice)
            this->bufferIdToGroupId[bid] = this->partitionCount_taskGroupMap[this->nextGroupId];


        groupLock.unlock();

        this->nextGroupId ++;
        this->maxPartitionNumber += partitionCount;

    }
    void enqueueGlobalPages(vector<shared_ptr<DataPage>> pages)
    {

        for(int i = 0 ; i < pages.size() ; i++) {
            if(pages[i]->isEndPage())
            {
                this->setEndPageSignal(pages[i]);
                this->allPages.push_back(pages[i]);
            }
            else
                this->allPages.push_back(pages[i]);
        }

    }
    void setEndPageSignal(shared_ptr<DataPage> page)
    {
        this->endPageFound = true;
        this->endPageAddr = page;
    }
    void balanceGlobalPages()
    {

        groupLock.lock();
        auto groupMap = this->partitionCount_taskGroupMap;
        groupLock.unlock();
        for(auto group : groupMap) {

            if(!group.second->isReceivedGlobalPages())
            {
                group.second->enqueuePages(this->allPages);
                group.second->receiveGlobalPages();
            }
        }


    }

    void viewTaskGroups()
    {
        for(auto group : this->partitionCount_taskGroupMap)
        {
            spdlog::debug(to_string(group.first) + "|"+ to_string(group.second->getStartPartitionNumber())+"|"+
                                                                                                           to_string(group.second->getPartitionCount()));
        }

    }


    void closeGroupInOnceType(int groupId)
    {


        for(auto group : this->partitionCount_taskGroupMap)
        {
            if(group.first == groupId)
            {
                if(group.second->getGroupStatus() == false)
                    return;
                viewTaskGroups();
                spdlog::debug("Close task group Id "+ to_string(group.first));
                group.second->enqueuePages({this->tempEndPage});
                return;
            }

        }



    }

    shared_ptr<PartitionCount_BuffersMap> getGroupInfoByBufferId(int bufferId)
    {
        for(auto pt : this->partitionCount_taskGroupMap)
        {
            if(pt.second->isBufferInGroup(bufferId))
            {

                return pt.second;
            }
        }
        return NULL;
        //cout << "partitionTaskGroup ERROR ! Cannot identify this bufferId [" <<bufferId <<"]" <<endl;
        //exit(0);
    }


    vector<shared_ptr<PartitionCount_BuffersMap>> selectGroups(bool repeat)
    {
        vector<shared_ptr<PartitionCount_BuffersMap>> maps;
        groupLock.lock();
        auto groupMap = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        if(repeat == true)
        {

            for(auto map : groupMap)
            {
                maps.push_back(map.second);
            }

        }
        else {


            int select;
            select = this->nextSelectedGroupId;

            while(1) {

                if(groupMap[select] != NULL && groupMap[select]->getGroupStatus() == true && groupMap[select]->getBlockQueueSize() == 0 &&
                groupMap[select]->getGroupStorePagesSize() < this->taskGroupPagesLimits[select])
                    break;

                if(groupMap[select] == NULL)
                    groupMap[select] = this->partitionCount_taskGroupMap[select];

                select =this->nextSelectedGroupId;
                this->nextSelectedGroupId++;
                if (this->nextSelectedGroupId >= this->nextGroupId)
                    this->nextSelectedGroupId = 0;
            }
            this->preSelectedGroupId = select;

            this->nextSelectedGroupId++;
            if (this->nextSelectedGroupId >= this->nextGroupId)
                this->nextSelectedGroupId = 0;


            maps.push_back(groupMap[select]);
        }
        return maps;
    }

    vector<int> getBufferIdsByGroupId(int groupId)
    {
        return this->partitionCount_taskGroupMap[0]->getGroupBufferIds();
    }

    void addEndPageForUnselectedGroup(shared_ptr<DataPage> page)
    {
        groupLock.lock();
        for(auto group : this->partitionCount_taskGroupMap)
        {
            if(group.first != this->preSelectedGroupId) {
                if (group.second->getGroupStatus() == true)
                    group.second->enqueuePages({page});
            }
        }
        groupLock.unlock();
    }
    bool isCacheExist(int bufferId)
    {
        bool flag = false;
        buffersLock->lock();
        int pCount = this->bufferIdToPartitionCount[bufferId];
        flag = this->prc->partitionCacheExist(pCount);
        buffersLock->unlock();

        return flag;

    }


    vector<shared_ptr<DataPage>> getPages(string bufferId,long token,int pageNums) {

        vector<shared_ptr<DataPage>> result;


        int bufferIdCount = this->buffers->count(bufferId);




        if(bufferIdCount == 0)
        {
            this->currentPages =- result.size();
            return result;
        }
        else
        {

       //     if(this->repeatable)
       //         this->processSomePagesByBufferId(atoi(bufferId.c_str()),pageNums*100);
       //     else
       //         this->processSomePagesByBufferId(atoi(bufferId.c_str()),pageNums*100);
            if(!this->repeatable)
                result = (*buffers)[bufferId]->getPages(pageNums);
            else
                result =  (*buffers)[bufferId]->getPages();
        }

        this->tuneBufferCapacity("consumer");

        this->currentPages =- result.size();




     if(this->bufferIdToGroupId.count(atoi(bufferId.c_str())) > 0) {
         if(this->bufferIdToGroupId[atoi(bufferId.c_str())] != NULL) {
             this->bufferIdToGroupId[atoi(bufferId.c_str())]->addTaskGroupTraffic(result.size());
                     resetBufferSizeByTrafficRate();
         }
     }
        return result;

    }

    vector<int> getHavingSpaceTaskGroups()
    {
        vector<int> ids;
        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        for(auto group : temp) {

            if (group.second->getGroupStatus() == true) {
                int ss = group.second->getGroupStorePagesSize();
                if(ss < this->taskGroupPagesLimits[group.first])
                    ids.push_back(group.first);
            }
        }
        return ids;
    }

    void resetBufferSizeByTrafficRate()
    {
        timelock.lock();

        if (this->start == NULL) {
            this->start = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());

        }else {
            shared_ptr<std::chrono::system_clock::time_point> circle = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());


            double duration_millsecond = std::chrono::duration<double, std::milli>(*circle - *this->start).count();

            if(duration_millsecond > bufferTuneCircle)
            {
                resetTaskGroupBufferSize();
                this->start = NULL;
            }
        }
        timelock.unlock();
    }


    void tuneBufferCapacity(string role)
    {
        if(role == "producer" && this->curRole == producer)
            return;
        if(role == "consumer" && this->curRole == consumer)
            return;

        if(role == "producer" && this->isFull()) {
            this->curRole = producer;
        //    this->reduceTaskGroupBufferSize();
        //    this->taskContext->addBufferSizeTurnDownCounter();
        }

        if(role == "consumer" && this->isEmpty()) {
            this->curRole = consumer;
            this->expandTaskGroupBufferSize();
            if(this->taskContext != NULL)
                this->taskContext->addBufferSizeTurnUpCounter();
        }

        if(role == "producer")
            this->curRole = producer;
        if(role == "consumer")
            this->curRole = consumer;


    }
    void expandTaskGroupBufferSize()
    {
        vector<int> ids;
        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        for(auto group : temp) {

            if(group.second->getGroupStatus() == true && group.second->getGroupStorePagesSize() == 0) {
                this->taskGroupPagesLimits[group.first] += this->taskGroupChangeSize;
            }
        }
    }


    void resetTaskGroupBufferSize()
    {
        vector<int> ids;
        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        for(auto group : temp) {
            if (group.second->getGroupStatus() == true) {
                int cap = group.second->getTaskGroupTraffic();
                if(cap < 1)
                    cap = 1;
                this->taskGroupPagesLimits[group.first] = cap;
                if(group.second->getTaskGroupTraffic() < this->taskGroupPagesLimits[group.first])
                    if(this->taskContext != NULL)
                        this->taskContext->addBufferSizeTurnDownCounter();
                group.second->resetTaskGroupTraffic();
            }
        }
    }


    void reduceTaskGroupBufferSize()
    {
        vector<int> ids;
        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        for(auto group : temp) {

            if(group.second->getGroupStatus() == true) {
                int ss = group.second->getGroupStorePagesSize();

                if (ss < this->taskGroupPagesLimits[group.first] && group.second->getBlockQueueSize() == 0) {
                    this->taskGroupPagesLimits[group.first] -= this->taskGroupChangeSize;
                    if(this->taskGroupPagesLimits[group.first] <= 1)
                        this->taskGroupPagesLimits[group.first] = 1;
                }
            }
        }

    }

    void adjustEmptyTaskGroupsSize()
    {
        vector<int> ids;
        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        for(auto group : temp) {

            if(group.second->getGroupStatus() == true && group.second->getGroupStorePagesSize() == 0) {
                this->taskGroupPagesLimits[group.first] += this->taskGroupChangeSize;
            }
        }

    }

    bool havingEmptyTaskGroup()
    {
        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        for(auto group : temp) {

            if(group.second->getGroupStatus() == true) {
                int ss = group.second->getGroupStorePagesSize();
                if(ss == 0)
                    return true;
            }
        }
        return false;
    }

    bool isFull()
    {

        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        int allClosedNum = 0;


        vector<shared_ptr<PartitionCount_BuffersMap>> havingSpaceGroups;

        for(auto group : temp) {

            if(group.second->getGroupStatus() == true) {
                int ss = group.second->getGroupStorePagesSize();

                if (ss < this->taskGroupPagesLimits[group.first] && group.second->getBlockQueueSize() == 0)
                    havingSpaceGroups.push_back(group.second);
            }
            else
                allClosedNum++;

        }

        if(allClosedNum == temp.size())
            return false;

        if(havingSpaceGroups.size() > 0)
            return false;
        else
            return true;

    }

    bool isEmpty()
    {
     //   if(this->havingEmptyTaskGroup())
     //       this->adjustEmptyTaskGroupsSize();

        map<int,shared_ptr<PartitionCount_BuffersMap>> temp;
        groupLock.lock();
        temp = this->partitionCount_taskGroupMap;
        groupLock.unlock();

        int total = 0;
        for(auto group : temp) {
            total+=group.second->getGroupStorePagesSize();
        }

        if(total == 0)
            return true;
        else
            return false;
    }


};

#endif //OLVP_TASKBUFFERGROUP_HPP
