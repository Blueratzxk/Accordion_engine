//
// Created by zxk on 9/1/25.
//

#ifndef OLVP_SIDEWAYEXCHANGESYSTEM_HPP
#define OLVP_SIDEWAYEXCHANGESYSTEM_HPP

#include "SidewayDataExchangeScheduler.hpp"

class SidewayExchangeSystem
{
    vector<StageExecutionAndScheduler> stageExeSchedulers;

    list<shared_ptr<SidewayDataExchangeScheduler>> sidewayExchangeTasks;


public:
    SidewayExchangeSystem(vector<StageExecutionAndScheduler> stageExeSchedulers){
        this->stageExeSchedulers = stageExeSchedulers;
    }

    shared_ptr<SqlStageExecution> getStageExecution(int stageId)
    {
        for(auto item : stageExeSchedulers)
            if(item.getStageExecution()->getStageId().getId() == stageId)
                return item.getStageExecution();
        return NULL;
    }
    shared_ptr<StageScheduler> getStageScheduler(int stageId)
    {
        for(auto item : stageExeSchedulers)
            if(item.getStageExecution()->getStageId().getId() == stageId)
                return item.getStageScheduler();
        return NULL;
    }
    shared_ptr<StageLinkage> getStageLinkage(int stageId)
    {
        for(auto item : stageExeSchedulers)
            if(item.getStageExecution()->getStageId().getId() == stageId)
                return item.getStageLinkage();
        return NULL;
    }


    void submitSidewayExchangeTask(int stageId,vector<int> taskIds,SidewayDataExchangeScheduler::MissionType type, SidewayDataExchangeScheduler::DataExchangeMode mode=SidewayDataExchangeScheduler::ONE_TO_ONE)
    {

        auto stageExe = this->getStageExecution(stageId);
        auto stageLink = this->getStageLinkage(stageId);
        auto stageSche =this->getStageScheduler(stageId);


        vector<int> rightIds;
        int rightId;
        for(auto &tid : taskIds) {
            if (!checkTaskIds(stageId, tid, rightId))
                tid = rightId;
        }
        auto sidewayTask = make_shared<SidewayDataExchangeScheduler>(type,stageExe,stageSche,stageLink,taskIds);
        this->sidewayExchangeTasks.push_back(sidewayTask);

        if(sidewayTask->schedule())
            sidewayTask->releaseMonitor();
    }


    void submitSidewayExchangeTask(int stageId,vector<int> taskIds,SidewayDataExchangeScheduler::MissionType type, SidewayDataExchangeScheduler::DataExchangeMode mode,int targetTaskNumber)
    {

        auto stageExe = this->getStageExecution(stageId);
        auto stageLink = this->getStageLinkage(stageId);
        auto stageSche =this->getStageScheduler(stageId);


        vector<int> rightIds;
        int rightId;
        for(auto &tid : taskIds) {
            if (!checkTaskIds(stageId, tid, rightId))
                tid = rightId;
        }
        auto sidewayTask = make_shared<SidewayDataExchangeScheduler>(type,stageExe,stageSche,stageLink,taskIds);
        this->sidewayExchangeTasks.push_back(sidewayTask);

        if(mode == SidewayDataExchangeScheduler::MANY_TO_MANY)
            sidewayTask->setManyToManyMode(targetTaskNumber);

        if(sidewayTask->schedule())
            sidewayTask->releaseMonitor();
    }


    bool checkTaskIds(int stageId,int taskId,int &id)
    {
        auto stageExe = this->getStageExecution(stageId);
        auto handle = stageExe->getFragment()->getPartitionHandle();
        if(handle != NULL && handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0) {
            if (static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType ==
                SystemPartitioningHandle::SINGLE) {
                int curTaskid = stageExe->getMaxTaskId();
                if (taskId != curTaskid) {
                    id = curTaskid;
                    return false;
                }
            }
        }

        return true;
    }

    bool isAllSidewayExchangeTasksOk()
    {
        for(auto item : this->sidewayExchangeTasks)
            if(!item->isSchedulingFinished())
                return false;

        return true;
    }
};


#endif //OLVP_SIDEWAYEXCHANGESYSTEM_HPP
