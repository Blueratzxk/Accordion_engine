//
// Created by zxk on 8/26/25.
//

#ifndef OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP
#define OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP


class SidewayDataExchangeScheduler : enable_shared_from_this<SidewayDataExchangeScheduler>
{


public:
    enum MissionType {
        OPERATOR_MIGRATION,
        BUFFER_MIGRATION,
        OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN,
        OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL,
        CLOSE_AND_CREATE,
        NODE_STATUS_RESETTING
    };

    enum DataExchangeMode
    {
        ONE_TO_ONE,
        MANY_TO_MANY
    };

    class SidewayMonitorPanel
    {
        MissionType missionType;
        set<string> newTaskIds;
        set<string> originTaskIds;

        SidewayDataExchangeScheduler *scheduler;

        bool originTasksFinishSignalSended = false;
    public:
        SidewayMonitorPanel(SidewayDataExchangeScheduler *scheduler,MissionType missionType,set<string> newTaskIds,set<string> originTaskIds){

            this->missionType = missionType;
            this->newTaskIds = newTaskIds;
            this->originTaskIds = originTaskIds;
            this->scheduler = scheduler;
        }

        set<string> getNewTaskIds(){return this->newTaskIds;}
        set<string> getOriginTaskIds(){return this->originTaskIds;}

        bool isSchedulingFinished()
        {
            if(this->missionType == OPERATOR_MIGRATION || this->missionType == OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL) {
                if (this->scheduler->opsNeededToMigrate.contains("LookupJoinOperator")) {

                    bool buildComplete = true;
                    if(this->scheduler->sidewayPreparationResponse->hasNotBuildCompleteState())
                            buildComplete = false;

                    if(!buildComplete)
                    {
                        if(!this->originTasksFinishSignalSended)
                            for (auto task: this->originTaskIds) {
                                TaskId id;
                                this->scheduler->sqlStageExecution->finishTaskByTaskId(id.StringToObject(task)->getId());
                            }

                        for (auto task: this->originTaskIds)
                            if (!this->scheduler->sqlStageExecution->isTaskFinished(task))
                                return false;
                    }
                    else {
                        for (auto task: this->newTaskIds)
                            if (!this->scheduler->sqlStageExecution->isTaskDependenciesSatisfied(task))
                                return false;

                        if(this->missionType != OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL)
                        if(!this->originTasksFinishSignalSended)
                            for (auto task: this->originTaskIds) {
                                TaskId id;
                                this->scheduler->sqlStageExecution->finishTaskByTaskId(id.StringToObject(task)->getId());
                            }
                        if(this->missionType != OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL)
                        for (auto task: this->originTaskIds) {
                            if (!this->scheduler->sqlStageExecution->isTaskFinished(task)) {
                                spdlog::info(task + " is finished!");
                                return false;
                            }
                        }
                    }

                } else if (this->scheduler->opsNeededToMigrate.contains("FinalAggregationOperator")) {

                    if(!this->originTasksFinishSignalSended)
                        for (auto task: this->originTaskIds) {
                            TaskId id;
                            this->scheduler->sqlStageExecution->finishTaskByTaskId(id.StringToObject(task)->getId());
                        }

                    for (auto task: this->originTaskIds)
                        if (!this->scheduler->sqlStageExecution->isTaskFinished(task))
                            return false;
                }
            }
            else if(this->missionType == BUFFER_MIGRATION)
            {
                for (auto task: this->newTaskIds)
                    if (!this->scheduler->sqlStageExecution->isTaskFinished(task))
                        return false;
            }
            else if(this->missionType == CLOSE_AND_CREATE)
            {
                if(!this->originTasksFinishSignalSended)
                    for (auto task: this->originTaskIds) {
                        TaskId id;
                        this->scheduler->sqlStageExecution->finishTaskByTaskId(id.StringToObject(task)->getId());
                    }

                for (auto task: this->originTaskIds)
                    if (!this->scheduler->sqlStageExecution->isTaskFinished(task))
                        return false;
            }
            else if(this->missionType == OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN)
            {
                bool buildComplete = true;
                if(this->scheduler->sidewayPreparationResponse->hasNotBuildCompleteState())
                    buildComplete = false;

                if(!buildComplete)
                {
                    if(!this->originTasksFinishSignalSended)
                        for (auto task: this->originTaskIds) {
                            TaskId id;
                            this->scheduler->sqlStageExecution->finishTaskByTaskId(id.StringToObject(task)->getId(),id.StringToObject(task)->getGeneration());
                        }

                    for (auto task: this->originTaskIds)
                        if (!this->scheduler->sqlStageExecution->isTaskFinished(task))
                            return false;
                }

                else
                {
                    for (auto task: this->newTaskIds)
                        if (!this->scheduler->sqlStageExecution->isTaskDependenciesSatisfied(task))
                            return false;

                    if(!this->originTasksFinishSignalSended)
                        for (auto task: this->originTaskIds) {
                            TaskId id;
                            this->scheduler->sqlStageExecution->finishTaskByTaskId(id.StringToObject(task)->getId(),id.StringToObject(task)->getGeneration());
                        }

                    for (auto task: this->originTaskIds) {
                        if (!this->scheduler->sqlStageExecution->isTaskFinished(task)) {
                            spdlog::info(task + " is finished!");
                            return false;
                        }
                    }
                }
            }

            this->scheduler->setFinishTime();

            return true;
        }

    };


private:
    MissionType missionType;
    shared_ptr<SqlStageExecution> sqlStageExecution;
    shared_ptr<StageScheduler> stageScheduler;
    shared_ptr<StageLinkage> stageLinkage;
    vector<int> taskIds; //task needed to migrate

    DataExchangeMode dataExchangeMode = ONE_TO_ONE;

    bool schedulingFinished = false;

    list<string> monitoredTaskIds;

    shared_ptr<SidewayMonitorPanel> sidewayMonitorPanel;

    map<string,string> migratableOperators = {
            {"FinalAggregationOperator","FinalAggregationNode"},
            {"LookupJoinOperator","LookupJoinNode"}
    };

    set<string> opsNeedInterTaskExchangeService
    {
            "FinalAggregationOperator"
    };


    set<string> opsNeededToMigrate;

    map<string,set<string>> sourceIdMap;

    shared_ptr<SidewayPreparationResponse> sidewayPreparationResponse;

    int targetTaskNumber = 0;


    shared_ptr<std::chrono::system_clock::time_point> startSchedulingTime = NULL;
    shared_ptr<std::chrono::system_clock::time_point> finishedSchedulingTime = NULL;


    string nodeDrainingMissionHelper_nodeUrl;


    set<string> newTaskIdStrs;
    set<string> originTaskIdStrs;

public:

    SidewayDataExchangeScheduler(MissionType type,shared_ptr<SqlStageExecution> sqlStageExecution,shared_ptr<StageScheduler> stageScheduler,shared_ptr<StageLinkage> stageLinkage,vector<int> taskIds)
    {
        this->missionType = type;
        this->sqlStageExecution = sqlStageExecution;
        this->stageScheduler = stageScheduler;
        this->stageLinkage = stageLinkage;
        this->taskIds = taskIds;
        this->sidewayPreparationResponse = make_shared<SidewayPreparationResponse>();
    }

    SidewayDataExchangeScheduler(MissionType type,string nodeUrl)
    {
        this->missionType = type;
        this->nodeDrainingMissionHelper_nodeUrl = nodeUrl;
        this->sidewayPreparationResponse = make_shared<SidewayPreparationResponse>();
    }

    MissionType getMissionType(){
        return this->missionType;
    }
    string getNodeDrainingMissionHelper_nodeUrl()
    {
        return this->nodeDrainingMissionHelper_nodeUrl;
    }


    void setStartTime()
    {
        if(needRecordInfo() && this->startSchedulingTime == NULL)
            this->startSchedulingTime = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());
    }
    void setFinishTime()
    {
        if(needRecordInfo() && this->finishedSchedulingTime == NULL)
            this->finishedSchedulingTime = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());
    }

    string getMigrationType(){

        if(this->missionType == OPERATOR_MIGRATION)
            return "OPERATOR_MIGRATION";
        else if(this->missionType == BUFFER_MIGRATION)
            return "BUFFER_MIGRATION";
        else if(this->missionType == OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN)
            return "OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN";
        else if(this->missionType == CLOSE_AND_CREATE)
            return "CLOSE_AND_CREATE";
        else if(this->missionType == OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL)
            return "OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL";
        else
            return "Unknown Migration Type!";

    }


    string getStartSchedulingTime (){

        return TimeCommon::getTimeStamp(this->startSchedulingTime);
    }
    string getFinishedSchedulingTime (){
        return TimeCommon::getTimeStamp(this->finishedSchedulingTime);
    }

    void setManyToManyMode(int targetTaskNumber)
    {
        this->targetTaskNumber = targetTaskNumber;
        this->dataExchangeMode = MANY_TO_MANY;
    }

    set<string> analyzeTaskMigratableOperators()
    {
        set<string> results;
        auto fragment = sqlStageExecution->getFragment();

        vector<PlanNode*> nodes;
        for(auto op : migratableOperators) {
            fragment->findNodeByNodeName(op.second, nodes);
            if(!nodes.empty())
                results.insert(op.first);
            nodes.clear();
        }
        return results;
    }


    bool migratedOperatorPreparation(int taskId)
    {

        opsNeededToMigrate = analyzeTaskMigratableOperators();
        if(opsNeededToMigrate.empty())
            return false;

        string parameters;
        if(opsNeededToMigrate.contains("LookupJoinOperator"))
            parameters = "DIRECT_HASHTABLE_INSTALL";
        if(this->missionType == OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL)
            parameters = "DIRECT_HASHTABLE_INSTALL";



        auto result = sqlStageExecution->taskMigrationPreparation(taskId,opsNeededToMigrate,parameters);
        if(result == NULL) {
            spdlog::info("Migrate operator preparation: rejected!");
            return false;
        }
        spdlog::info(result->getMessage());

        this->sidewayPreparationResponse = result->getSidewayPreparationResponse();
        if(this->sidewayPreparationResponse == NULL)
            this->sidewayPreparationResponse = make_shared<SidewayPreparationResponse>();

        return result->getStatus();
    }

    bool addTasksWithConditionExecution(int taskId)
    {

        opsNeededToMigrate = analyzeTaskMigratableOperators();
        set<string> needExchangeService;
        for(auto op : opsNeededToMigrate)
            if(this->opsNeedInterTaskExchangeService.contains(op))
                needExchangeService.insert(op);

        if(this->sidewayPreparationResponse->hasNotBuildCompleteState())
            return closeOriginAndCreateNewOne(taskId);

        string parameters;
        if(this->missionType == OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL) {
            parameters = "DIRECT_HASHTABLE_INSTALL";
            needExchangeService.insert("LookupJoinOperator");
        }
        if(opsNeededToMigrate.contains("LookupJoinOperator")) {
            parameters = "DIRECT_HASHTABLE_INSTALL";
            needExchangeService.insert("LookupJoinOperator");
        }


        string taskIdString = "";
        string ip;
        string port;
        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();

        for(auto task : tasks)
            if(task->getTaskId()->getId() == taskId) {
                taskIdString = task->getTaskId()->ToString();
                ip = task->getIP();
                port = task->getPORT();
            }

        MigratedOperators migratedOperators(opsNeededToMigrate,this->sidewayPreparationResponse,needExchangeService,taskIdString,ip,port);

        if(this->stageScheduler->getSchedulerType() != "NormalStageScheduler") {
            spdlog::error("MigrateBuffers: don't support migrating buffers for " + this->stageScheduler->getSchedulerType() + "!");
            return false;
        }



        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(stageScheduler))->addConcurrentForInterTaskMission(
                make_shared<TaskExecutionCondition>(TaskExecutionCondition::OPERATOR_MIGRATION,parameters,migratedOperators));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();

        for(auto newTask : newTasks)
            spdlog::info("Origin Task "+ to_string(taskId)+" migrate to Task "+newTask->getTaskId()->ToString()+" scheduled node is "+newTask->getIP());

        originTaskIdStrs.insert(taskIdString);

        for(auto task : newTasks) {
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());
            newTaskIdStrs.insert(task->getTaskId()->ToString());
        }


        this->stageLinkage->processScheduleResultsToAddConcurrent(newTasks);

        //this->sqlStageExecution->finishTaskByTaskId(taskId);

        //this->sidewayMonitorPanel = make_shared<SidewayMonitorPanel>(this,this->missionType,newTaskIdStrs,originTaskIdStrs);

        return true;
    }

    set<string> getNewTaskIds()
    {
        set<string> ids = this->sidewayMonitorPanel->getNewTaskIds();
        set<string> tids;
        TaskId Tid;
        for(auto id : ids) {
            if (this->missionType == OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN) {

                if (Tid.StringToObject(id)->getStageExecutionId().getId() > 0) {
                    tids.insert(to_string(Tid.StringToObject(id)->getId()) + "(" +
                                to_string(Tid.StringToObject(id)->getStageExecutionId().getId()) + ")");
                } else
                    tids.insert(to_string(Tid.StringToObject(id)->getId()));
            } else
                tids.insert(to_string(Tid.StringToObject(id)->getId()));
        }
        return tids;
    }
    set<string> getOriginTaskIds()
    {
        set<string> ids = this->sidewayMonitorPanel->getOriginTaskIds();
        set<string> tids;
        TaskId Tid;
        for(auto id : ids) {

            if(this->missionType == OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN) {

                if(Tid.StringToObject(id)->getStageExecutionId().getId() > 0)
                {
                    tids.insert(to_string(Tid.StringToObject(id)->getId()) + "(" +
                               to_string(Tid.StringToObject(id)->getStageExecutionId().getId()) + ")");
                }
                else
                    tids.insert(to_string(Tid.StringToObject(id)->getId()));
            }
            else
                tids.insert(to_string(Tid.StringToObject(id)->getId()));
        }
        return tids;
    }

    bool closeOriginAndCreateNewOne(int taskId)
    {

        string taskIdString = "";
        string ip;
        string port;
        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();

        for(auto task : tasks)
            if(task->getTaskId()->getId() == taskId) {
                taskIdString = task->getTaskId()->ToString();
                ip = task->getIP();
                port = task->getPORT();
            }

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(stageScheduler))->addConcurrentForInterTaskMission(NULL);
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();

        for(auto newTask : newTasks)
            spdlog::info("Task "+newTask->getTaskId()->ToString()+" scheduled node is "+newTask->getIP());


        originTaskIdStrs.insert(taskIdString);

        for(auto task : newTasks) {
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());
            newTaskIdStrs.insert(task->getTaskId()->ToString());
        }


        this->stageLinkage->processScheduleResultsToAddConcurrent(newTasks);

        this->sqlStageExecution->finishTaskByTaskId(taskId);

        //this->sidewayMonitorPanel = make_shared<SidewayMonitorPanel>(this,CLOSE_AND_CREATE,newTaskIdStrs,originTaskIdStrs);

        return true;
    }



    bool cloneTasksWithConditionExecution(int taskId)
    {

        opsNeededToMigrate = analyzeTaskMigratableOperators();
        set<string> needExchangeService;
        for(auto op : opsNeededToMigrate)
            if(this->opsNeedInterTaskExchangeService.contains(op))
                needExchangeService.insert(op);

        if(this->sidewayPreparationResponse->hasNotBuildCompleteState()) {
            auto sources = this->sidewayPreparationResponse->getNotBuildCompleteStateSources();
            for(auto source : sources)
                needExchangeService.insert(source);
        }
        string taskIdString = "";
        string ip;
        string port;
        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();


        auto task = sqlStageExecution->findMaxGenerationTask(taskId);
        spdlog::info("Clone task id:"+task->getTaskId()->ToString());
        taskIdString = task->getTaskId()->ToString();
        ip = task->getIP();
        port = task->getPORT();


        MigratedOperators migratedOperators(opsNeededToMigrate,this->sidewayPreparationResponse,needExchangeService,taskIdString,ip,port);

        if(this->stageScheduler->getSchedulerType() != "NormalStageScheduler") {
            spdlog::error("MigrateBuffers: don't support migrating buffers for " + this->stageScheduler->getSchedulerType() + "!");
            return false;
        }

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(stageScheduler))->cloneTask(taskId,
                make_shared<TaskExecutionCondition>(TaskExecutionCondition::OPERATOR_MIGRATION,"",migratedOperators));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();


        originTaskIdStrs.insert(taskIdString);

        for(auto task : newTasks) {
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());
            newTaskIdStrs.insert(task->getTaskId()->ToString());
        }

        this->stageLinkage->processScheduleResultsToAddConcurrent(newTasks);



        //this->sidewayMonitorPanel = make_shared<SidewayMonitorPanel>(this,this->missionType,newTaskIdStrs,originTaskIdStrs);

        return true;
    }

    bool migrateOperators(int taskId)
    {
        if(migratedOperatorPreparation(taskId)) {
            return addTasksWithConditionExecution(taskId);
        }
        else
        {
            if(opsNeededToMigrate.empty())
                return closeOriginAndCreateNewOne(taskId);
        }
        return false;
    }

    bool migrateBuffers(int taskId) {

        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();
        shared_ptr<HttpRemoteTask> oldTaskPtr = NULL;
        string taskIdString;
        for (auto task: tasks)
            if (task->getTaskId()->getId() == taskId) {
                oldTaskPtr = task;
                taskIdString = task->getTaskId()->ToString();
            }
        if(oldTaskPtr == NULL)
            return false;

        TaskId taskIdTemp;
        MigratedBufferAddress migratedBufferAddress({taskIdTemp.Serialize(*oldTaskPtr->getTaskId())},{oldTaskPtr->getIP()},{"9081"},{"-1"});
        shared_ptr<TaskExecutionCondition> condition = make_shared<TaskExecutionCondition>(TaskExecutionCondition::BUFFER_MIGRATION,"",migratedBufferAddress);


        if(this->stageScheduler->getSchedulerType() != "NormalStageScheduler") {
            spdlog::error("MigrateBuffers: don't support migrating buffers for " + this->stageScheduler->getSchedulerType() + "!");
            return false;
        }

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(this->stageScheduler)->addConcurrentForInterTaskMission(condition));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();

        for(auto task : newTasks)
            task->setMigratedBufferTask();

        for(auto newTask : newTasks)
            spdlog::info("Task "+newTask->getTaskId()->ToString()+" scheduled node is "+newTask->getIP());


        originTaskIdStrs.insert(taskIdString);

        for(auto task : newTasks) {
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());
            newTaskIdStrs.insert(task->getTaskId()->ToString());
        }

        spdlog::info(taskIdString+" is finished or not: "+ to_string(this->sqlStageExecution->isTaskFinished(taskIdString)));


        this->stageLinkage->processScheduleResultsToReplaceSourceTasks(newTasks,{taskId});


        //this->sidewayMonitorPanel = make_shared<SidewayMonitorPanel>(this,this->missionType,newTaskIdStrs,originTaskIdStrs);

        return true;
    }


    bool migrateBuffersManyToMany(vector<int> taskIds, int targetTaskNumber) {


        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();
        vector<shared_ptr<HttpRemoteTask>> originTaskPtrs;
        shared_ptr<HttpRemoteTask> oldTaskPtr = NULL;


        for(auto taskId : taskIds) {
            for (auto task: tasks)
                if (task->getTaskId()->getId() == taskId) {
                    originTaskPtrs.push_back(task);
                    originTaskIdStrs.insert(task->getTaskId()->ToString());
                }
        }
        if(originTaskPtrs.size() != taskIds.size())
            return false;

        TaskId taskIdTemp;
        MigratedBufferAddress migratedBufferAddress;

        for(auto originTask : originTaskPtrs) {
            migratedBufferAddress.addMigratedBufferAddress({ taskIdTemp.Serialize(*originTask->getTaskId()) }, {originTask->getIP()}, {"9081"}, {to_string(-1)});

        }

        shared_ptr<TaskExecutionCondition> condition = make_shared<TaskExecutionCondition>(TaskExecutionCondition::BUFFER_MIGRATION,"",migratedBufferAddress);


        vector<shared_ptr<HttpRemoteTask>> newTasks;

        if(this->stageScheduler->getSchedulerType() == "SourcePartitionedScheduler") {
            ScheduleResult result = (static_pointer_cast<SourcePartitionedScheduler>(this->stageScheduler)->addConcurrentForInterTaskMission(condition,targetTaskNumber));
            newTasks = result.getNewTasks();
        }
        else
        {
            ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(this->stageScheduler)->addConcurrentForInterTaskMission(condition,targetTaskNumber));
            newTasks = result.getNewTasks();
        }

        for(auto task : newTasks)
            task->setMigratedBufferTask();



        for(auto task : newTasks) {
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());
            newTaskIdStrs.insert(task->getTaskId()->ToString());
        }

        set<int> abandonedTaskIds;
        for(auto id : taskIds)
            abandonedTaskIds.insert(id);
        this->sqlStageExecution->abandonTasks(abandonedTaskIds);

        this->stageLinkage->processScheduleResultsToReplaceSourceTasks(newTasks,taskIds);

        //this->sidewayMonitorPanel = make_shared<SidewayMonitorPanel>(this,this->missionType,newTaskIdStrs,originTaskIdStrs);

        return true;


    }

    bool cloneTask(int taskId)
    {
        if(migratedOperatorPreparation(taskId)) {
            cloneTasksWithConditionExecution(taskId);
            return true;
        }
        return false;
    }

    bool schedule(int taskId)
    {
        if(this->missionType == OPERATOR_MIGRATION || this->missionType == OPERATOR_MIGRATION_DIRECT_HASH_TABLE_INSTALL)
            return migrateOperators(taskId);
        else if(this->missionType == BUFFER_MIGRATION)
            return migrateBuffers(taskId);
        else if(this->missionType == OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN)
            return cloneTask(taskId);
        else {
            spdlog::info("Unsupported sideway exchange type!");
            return false;
        }
    }

    bool needRecordInfo(){

        if(this->missionType == NODE_STATUS_RESETTING)
            return false;

        return true;
    }

    set<string> getMigratedOps()
    {
        return this->opsNeededToMigrate;
    }

    string getMigratedStageId()
    {
        return to_string(this->sqlStageExecution->getStageId().getId());
    }

    bool schedule()
    {

        bool status = true;
        set<bool> allStatus;

        if(this->missionType == BUFFER_MIGRATION && this->dataExchangeMode == MANY_TO_MANY)
        {
            auto re = migrateBuffersManyToMany(this->taskIds,this->targetTaskNumber);
            allStatus.insert(re);
        }
        else if(this->getMissionType() == NODE_STATUS_RESETTING) {
            string nodeUrl = this->getNodeDrainingMissionHelper_nodeUrl();
            ClusterServer::getNodesManager()->resetNodeAliveStatus(nodeUrl);
            set<string> nullSet;
            allStatus.insert(true);
        }
        else {
            for (auto taskId: this->taskIds){
                auto re = schedule(taskId);
                allStatus.insert(re);
            }
        }

        this->sidewayMonitorPanel = make_shared<SidewayMonitorPanel>(this,this->missionType,newTaskIdStrs,originTaskIdStrs);

        if(allStatus.contains(true))
            return true;

        return false;
    }

    bool isSchedulingFinished()
    {
        if(this->sidewayMonitorPanel == NULL)
            return false;
        return this->sidewayMonitorPanel->isSchedulingFinished();
    }


};



#endif //OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP
