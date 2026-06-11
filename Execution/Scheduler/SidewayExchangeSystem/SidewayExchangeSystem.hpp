//
// Created by zxk on 9/1/25.
//

#ifndef OLVP_SIDEWAYEXCHANGESYSTEM_HPP
#define OLVP_SIDEWAYEXCHANGESYSTEM_HPP

#include "SidewayDataExchangeScheduler.hpp"
#include "../SqlScheduleLock.hpp"

class SidewayExchangeSystem
{
    vector<StageExecutionAndScheduler> stageExeSchedulers;


    list<shared_ptr<SidewayDataExchangeScheduler>> sidewayExchangeTaskQueue;

    list<shared_ptr<SidewayDataExchangeScheduler>> finishedSidewayExchangeTasks;

    list<shared_ptr<SidewayDataExchangeScheduler>> sidewayExchangeTasks;

    mutex lock;

    atomic<bool> sidewayExecutorActivated = false;

    SimpleEvent missionsStatusEvent;
    shared_ptr<SqlScheduleLock> sqlScheduleLock;

    atomic<bool> nodeDrainingScheduleFinished = false;

    enum NodeDrainingState
    {
        START,FINISHED
    }DrainingState = FINISHED;

    list<pair<shared_ptr<std::chrono::system_clock::time_point>,shared_ptr<std::chrono::system_clock::time_point>>> nodeDrainingRecords;

    shared_ptr<std::chrono::system_clock::time_point> nodeDrainingStartTime = NULL;
    shared_ptr<std::chrono::system_clock::time_point> nodeDrainingFinishTime = NULL;

    map<int, shared_ptr<StageExecutionAndScheduler>> stageExecutionsMap;

public:
    SidewayExchangeSystem(vector<StageExecutionAndScheduler> stageExeSchedulers, map<int, shared_ptr<StageExecutionAndScheduler>> stageExecutionsMap, shared_ptr<SqlScheduleLock> sqlScheduleLock){
        this->stageExeSchedulers = stageExeSchedulers;
        this->sqlScheduleLock = sqlScheduleLock;
        this->stageExecutionsMap = stageExecutionsMap;
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
    shared_ptr<StageExecutionAndScheduler> getStageExecutionAndSchedulerByStagId(int stageId)
    {
        if(this->stageExecutionsMap.count(stageId) > 0)
            return this->stageExecutionsMap[stageId];

        return NULL;
    }


    shared_ptr<StageExecutionAndScheduler> findRootTableScanStageForStage(string stageId) {
        shared_ptr<StageExecutionAndScheduler> es = this->getStageExecutionAndSchedulerByStagId(atoi(stageId.c_str()));

        while(!es->getStageLinkage()->getChildStages().empty())
        {
            auto stageExe = es->getStageLinkage()->getChildStages()[0];
            es = getStageExecutionAndSchedulerByStagId(stageExe->getStageId().getId());
        }
        return es;
    }


    void releaseExecutor()
    {
        thread executor(sidewayExecutor,this);
        executor.detach();
    }

    bool noSidewayTasks()
    {
        bool state = false;
        lock.lock();
        if(this->sidewayExchangeTaskQueue.empty() && (!this->sidewayExecutorActivated))
            state = true;
        lock.unlock();
        return state;
    }

    void listenAllMissionsStatus()
    {
        this->missionsStatusEvent.listen();
    }

    void notifyAllMissionsStatus()
    {
        this->missionsStatusEvent.notify();
    }

    static void sidewayExecutor(SidewayExchangeSystem *system){

        system->sidewayExecutorActivated = true;

        shared_ptr<SidewayDataExchangeScheduler> scheduler = NULL;

        while(true) {

            system->lock.lock();
            bool queueEmpty = system->sidewayExchangeTaskQueue.empty();

            system->lock.unlock();
            if(queueEmpty) {
                system->notifyAllMissionsStatus();
                break;
            }


            spdlog::info("Sideway exchange schedule start!");
            system->lock.lock();
            scheduler = system->sidewayExchangeTaskQueue.front();

            system->sidewayExchangeTaskQueue.pop_front();
            system->lock.unlock();


            if(scheduler->schedule())
                while(!scheduler->isSchedulingFinished()) sleep_for(std::chrono::milliseconds(200));


            spdlog::info("Sideway exchange schedule finished!");

            if(scheduler->needRecordInfo())
                system->finishedSidewayExchangeTasks.push_back(scheduler);

            if(system->sidewayExchangeTaskQueue.empty() && system->nodeDrainingScheduleFinished) {

                system->NodeDrainingFinished();
                system->sqlScheduleLock->sideway_unlock();
            }

            system->notifyAllMissionsStatus();

        }

        system->sidewayExecutorActivated = false;
    }


    void submitSidewayExchangeTask(int stageId,vector<int> taskIds,SidewayDataExchangeScheduler::MissionType type, SidewayDataExchangeScheduler::DataExchangeMode mode=SidewayDataExchangeScheduler::ONE_TO_ONE)
    {

        auto stageExe = this->getStageExecution(stageId);
        auto stageLink = this->getStageLinkage(stageId);
        auto stageSche =this->getStageScheduler(stageId);

        double timeLimit = this->findRootTableScanStageForStage(to_string(stageId))->getStageExecution()->getRemainingTime();



        vector<int> rightIds;
        int rightId;
        for(auto &tid : taskIds) {
            if (!checkTaskIds(stageId, tid, rightId))
                tid = rightId;
        }
        auto sidewayTask = make_shared<SidewayDataExchangeScheduler>(type,stageExe,stageSche,stageLink,taskIds,timeLimit);

        sidewayTask->setStartTime();


        string stroutput;
        for(auto id : taskIds)
            stroutput.append(to_string(id)).append(" ");
            spdlog::info("Submit sideway task! "+ getMigrationTypeStr(stageExe) +" "+stroutput);



        this->lock.lock();
        this->sidewayExchangeTaskQueue.push_back(sidewayTask);

        this->lock.unlock();

        if(!this->sidewayExecutorActivated)
            this->releaseExecutor();
    }

    nlohmann::json getSchedulingInfos()
    {

        nlohmann::json infos;
        for(auto schedule: this->finishedSidewayExchangeTasks)
        {
            nlohmann::json info;
            info["sidewayType"] = schedule->getMigrationType();
            info["stageId"] = schedule->getMigratedStageId();
            info["operators"] = schedule->getMigratedOps();
            info["start"] = schedule->getStartSchedulingTime();
            info["finish"] = schedule->getFinishedSchedulingTime();
            info["newTaskIds"] = schedule->getNewTaskIds();
            info["originTaskIds"] = schedule->getOriginTaskIds();
            info["estimatedMigrationTime"] = schedule->getEstimatedMigrationTime();
            infos.push_back(info);
        }

        for(auto record: this->nodeDrainingRecords)
        {
            nlohmann::json info;
            info["sidewayType"] = "NodeDraining";
            info["stageId"] = "NULL";
            info["operators"] = "NULL";
            info["start"] = TimeCommon::getTimeStamp(record.first);
            info["finish"] = TimeCommon::getTimeStamp(record.second);
            info["newTaskIds"] = "NULL";
            info["originTaskIds"] = "NULL";
            infos.push_back(info);
        }
        return infos;
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

        list<TaskId> newestTaskIds = stageExe->getNewestTaskGroup();
        vector<int> taskGroupIds;
        for(auto id : newestTaskIds)
            taskGroupIds.push_back(id.getId());


        double timeLimit = this->findRootTableScanStageForStage(to_string(stageId))->getStageExecution()->getRemainingTime();


        auto sidewayTask = make_shared<SidewayDataExchangeScheduler>(type,stageExe,stageSche,stageLink,taskIds,timeLimit);
        sidewayTask->setStartTime();
        this->sidewayExchangeTasks.push_back(sidewayTask);

        if(mode == SidewayDataExchangeScheduler::MANY_TO_MANY)
            sidewayTask->setManyToManyMode(targetTaskNumber);

        this->lock.lock();
        this->sidewayExchangeTaskQueue.push_back(sidewayTask);
        this->lock.unlock();

        if(!this->sidewayExecutorActivated)
            this->releaseExecutor();
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


    map<int,set<int>> getAllRunningStageTasksMap(string nodeUrl)
    {
        map<int,set<int>> stageTasksMap;

        for(auto stage : this->stageExeSchedulers)
        {
            auto taskIds = stage.getStageExecution()->getRunningTasksOnTheNode(nodeUrl);
            stageTasksMap[stage.getStageExecution()->getStageId().getId()] = taskIds;
        }

        return stageTasksMap;
    }

    map<shared_ptr<SqlStageExecution>,pair<set<int>,set<int>>> getStageTasksMap(string nodeUrl)
    {
        map<shared_ptr<SqlStageExecution>,pair<set<int>,set<int>>> stageTasksMap;

        for(auto stage : this->stageExeSchedulers)
        {
            set<int> active;
            set<int> closed;
            stage.getStageExecution()->getActiveAndClosedTasksOnTheNode(nodeUrl,active,closed);
            stageTasksMap[stage.getStageExecution()] = {active,closed};
        }

        return stageTasksMap;
    }

    SidewayDataExchangeScheduler::MissionType getMigrationType(shared_ptr<SqlStageExecution> sqlStageExecution)
    {
        SidewayDataExchangeScheduler::MissionType type = SidewayDataExchangeScheduler::OPERATOR_MIGRATION;
        auto handle = sqlStageExecution->getFragment()->getPartitionHandle();

        if(handle != NULL && handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0) {
            if (static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType ==
                SystemPartitioningHandle::HASH_SCALED) {
                type = SidewayDataExchangeScheduler::OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN;
            }
        }
        return type;
    }

    string getMigrationTypeStr(shared_ptr<SqlStageExecution> sqlStageExecution)
    {
        auto type = getMigrationType(sqlStageExecution);
        if(type == SidewayDataExchangeScheduler::OPERATOR_MIGRATION)
            return "OPERATOR_MIGRATION";
        else return "OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN";
    }

    void processActiveStageTaskMigration(string nodeUrl)
    {
        map<shared_ptr<SqlStageExecution>,pair<set<int>,set<int>>> stageTasksMap;
        stageTasksMap = getStageTasksMap(nodeUrl);

        for (auto it = stageTasksMap.begin(); it != stageTasksMap.end(); ) {
            if (it->first->getState()->isDone()) {
                it = stageTasksMap.erase(it);
            } else {
                ++it;
            }
        }

        for(auto stage : stageTasksMap)
        {
            vector<int> taskIds;

            string ids;
            auto active = stage.second.first;
            for(auto act : active) {
                ids.append(to_string(act)).append(" ");
                taskIds.push_back(act);
            }
            if(!ids.empty())
                spdlog::info("Stage:"+to_string(stage.first->getStageId().getId())+" migrate tasks:"+ids + " Type:"+getMigrationTypeStr(stage.first));

            if(!taskIds.empty())
                submitSidewayExchangeTask(stage.first->getStageId().getId(),taskIds, getMigrationType(stage.first));


        }

    }

    void submitTaskMigrationMission(int stageId,vector<int> taskIds)
    {
        auto stageExe = this->getStageExecution(stageId);
        submitSidewayExchangeTask(stageId,taskIds, getMigrationType(stageExe));
    }

    void submitBufferMigrationMission(int stageId,vector<int> taskIds,SidewayDataExchangeScheduler::DataExchangeMode mode,int targetTaskNums = 1)
    {
        auto stageExe = this->getStageExecution(stageId);
        if(mode == SidewayDataExchangeScheduler::ONE_TO_ONE)
            submitSidewayExchangeTask(stageId,taskIds,SidewayDataExchangeScheduler::BUFFER_MIGRATION);
        else
            submitSidewayExchangeTask(stageId,taskIds,SidewayDataExchangeScheduler::BUFFER_MIGRATION,SidewayDataExchangeScheduler::MANY_TO_MANY,targetTaskNums);
    }

    void processActiveStageBufferMigration(string nodeUrl)
    {
        map<shared_ptr<SqlStageExecution>,pair<set<int>,set<int>>> stageTasksMap;
        map<shared_ptr<SqlStageExecution>,pair<set<int>,set<int>>> allStageTasksMap;
        stageTasksMap = getStageTasksMap(nodeUrl);
        allStageTasksMap = stageTasksMap;


        for (auto it = stageTasksMap.begin(); it != stageTasksMap.end(); ) {
            if (it->first->getState()->isDone()) {
                it = stageTasksMap.erase(it);
            } else {
                ++it;
            }
        }


        for(auto stage : stageTasksMap)
        {
            string ids;

            auto linkage = this->getStageLinkage(stage.first->getStageId().getId());

            shared_ptr<SqlStageExecution> upstreamStage = NULL;
            if(linkage->getChildStages().size() > 1)
                upstreamStage = linkage->getChildStages().back();


            set<int> activeIds;
            set<int> closedIds;
            if(upstreamStage != NULL) {
                activeIds = allStageTasksMap[upstreamStage].first;
                closedIds = allStageTasksMap[upstreamStage].second;
            }

            for(auto ai : activeIds)
                ids.append("active:"+to_string(ai)).append(" ");

            for(auto ci : closedIds)
                ids.append("closed:"+to_string(ci)).append(" ");


            vector<int> bufferTasksNeededToMigrate;

            if(!ids.empty()) {
                spdlog::info("Stage:" + to_string(stage.first->getStageId().getId()) + " needs Stage:" +
                             to_string(upstreamStage->getStageId().getId()) + " migrate buffer tasks:" + ids +
                             " Type:" + getMigrationTypeStr(stage.first));

                vector<int> AllBufferTasksNeededToMigrate;
                for(auto id : closedIds)
                    AllBufferTasksNeededToMigrate.push_back(id);
                for(auto id : activeIds)
                    AllBufferTasksNeededToMigrate.push_back(id);


                auto newestBufferTasks = stage.first->getSourceTasks();

                set<int> newestBufferTaskIds;
                for(auto task : newestBufferTasks) {
                    if(task->getTaskId()->getStageId().getId() == upstreamStage->getStageId().getId())
                        newestBufferTaskIds.insert(task->getTaskId()->getId());
                }

                for(auto id : AllBufferTasksNeededToMigrate)
                    if(newestBufferTaskIds.contains(id))
                        bufferTasksNeededToMigrate.push_back(id);
            }



            if(!bufferTasksNeededToMigrate.empty() && !stage.first->getState()->isDone())
                submitSidewayExchangeTask(upstreamStage->getStageId().getId(),bufferTasksNeededToMigrate, SidewayDataExchangeScheduler::BUFFER_MIGRATION);
        }

    }

    void resetNodeAliveStatus(string nodeUrl)
    {

        auto resetNodeStatus = make_shared<SidewayDataExchangeScheduler>(SidewayDataExchangeScheduler::NODE_STATUS_RESETTING,nodeUrl,-1);
        this->lock.lock();
        this->sidewayExchangeTaskQueue.push_back(resetNodeStatus);
        this->lock.unlock();

        if(!this->sidewayExecutorActivated)
            this->releaseExecutor();
    }

    void NodeDrainingStart()
    {
        this->DrainingState = START;
        this->nodeDrainingStartTime =  make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());
        this->sqlScheduleLock->sideway_lock();
        this->nodeDrainingScheduleFinished = false;
    }
    void NodeDrainingFinished()
    {
        this->DrainingState = FINISHED;
        this->nodeDrainingFinishTime =  make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());
        this->nodeDrainingRecords.push_back({this->nodeDrainingStartTime,this->nodeDrainingFinishTime});
    }

    void processNodeDraining(string nodeUrl)
    {
        NodeDrainingStart();
        processActiveStageTaskMigration(nodeUrl);
        processActiveStageBufferMigration(nodeUrl);
        resetNodeAliveStatus(nodeUrl);

        this->nodeDrainingScheduleFinished = true;


    }

};


#endif //OLVP_SIDEWAYEXCHANGESYSTEM_HPP
