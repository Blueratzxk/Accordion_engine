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
        OPERATOR_MIGRATION_PARTITIONED_HASH_JOIN
    };

    enum DataExchangeMode
    {
        ONE_TO_ONE,
        MANY_TO_MANY
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

    int targetTaskNumber = 0;

public:

    SidewayDataExchangeScheduler(MissionType type,shared_ptr<SqlStageExecution> sqlStageExecution,shared_ptr<StageScheduler> stageScheduler,shared_ptr<StageLinkage> stageLinkage,vector<int> taskIds)
    {
        this->missionType = type;
        this->sqlStageExecution = sqlStageExecution;
        this->stageScheduler = stageScheduler;
        this->stageLinkage = stageLinkage;
        this->taskIds = taskIds;
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

        auto result = sqlStageExecution->taskMigrationPreparation(taskId,opsNeededToMigrate);
        if(result == NULL) {
            spdlog::info("Migrate operator preparation: rejected!");
            return false;
        }
        spdlog::info(result->getMessage());

        this->sourceIdMap = result->getSourceIdMap();
        return result->getStatus();
    }

    bool addTasksWithConditionExecution(int taskId)
    {

        opsNeededToMigrate = analyzeTaskMigratableOperators();
        set<string> needExchangeService;
        for(auto op : opsNeededToMigrate)
            if(this->opsNeedInterTaskExchangeService.contains(op))
                needExchangeService.insert(op);

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

        MigratedOperators migratedOperators(opsNeededToMigrate,this->sourceIdMap,needExchangeService,taskIdString,ip,port);

        if(this->stageScheduler->getSchedulerType() != "NormalStageScheduler") {
            spdlog::error("MigrateBuffers: don't support migrating buffers for " + this->stageScheduler->getSchedulerType() + "!");
            return false;
        }

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(stageScheduler))->addConcurrentForInterTaskMission(
                make_shared<TaskExecutionCondition>(TaskExecutionCondition::OPERATOR_MIGRATION,migratedOperators));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();

        for(auto task : newTasks)
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());

        this->stageLinkage->processScheduleResultsToAddConcurrent(newTasks);

        return true;
    }

    shared_ptr<HttpRemoteTask> findMaxGenerationTask(int taskId)
    {
        shared_ptr<HttpRemoteTask> result;
        int maxGen = -100;

        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();

        for(auto task : tasks)
            if(task->getTaskId()->getId() == taskId) {
                if(task->getTaskId()->getStageExecutionId().getId() > maxGen)
                {
                    maxGen = task->getTaskId()->getStageExecutionId().getId();
                    result = task;
                }
            }

        return result;
    }

    bool cloneTasksWithConditionExecution(int taskId)
    {

        opsNeededToMigrate = analyzeTaskMigratableOperators();
        set<string> needExchangeService;
        for(auto op : opsNeededToMigrate)
            if(this->opsNeedInterTaskExchangeService.contains(op))
                needExchangeService.insert(op);

        string taskIdString = "";
        string ip;
        string port;
        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();


        auto task = findMaxGenerationTask(taskId);
        spdlog::info("Clone task id:"+task->getTaskId()->ToString());
        taskIdString = task->getTaskId()->ToString();
        ip = task->getIP();
        port = task->getPORT();


        MigratedOperators migratedOperators(opsNeededToMigrate,this->sourceIdMap,needExchangeService,taskIdString,ip,port);

        if(this->stageScheduler->getSchedulerType() != "NormalStageScheduler") {
            spdlog::error("MigrateBuffers: don't support migrating buffers for " + this->stageScheduler->getSchedulerType() + "!");
            return false;
        }

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(stageScheduler))->cloneTask(taskId,
                make_shared<TaskExecutionCondition>(TaskExecutionCondition::OPERATOR_MIGRATION,migratedOperators));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();

        for(auto task : newTasks)
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());

        this->stageLinkage->processScheduleResultsToAddConcurrent(newTasks);

        return true;
    }

    bool migrateOperators(int taskId)
    {
        if(migratedOperatorPreparation(taskId)) {
            return addTasksWithConditionExecution(taskId);
        }
        return false;
    }

    bool migrateBuffers(int taskId) {

        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();
        shared_ptr<HttpRemoteTask> oldTaskPtr = NULL;
        for (auto task: tasks)
            if (task->getTaskId()->getId() == taskId) {
                oldTaskPtr = task;
            }
        if(oldTaskPtr == NULL)
            return false;

        TaskId taskIdTemp;
        MigratedBufferAddress migratedBufferAddress({taskIdTemp.Serialize(*oldTaskPtr->getTaskId())},{oldTaskPtr->getIP()},{"9081"},{"-1"});
        shared_ptr<TaskExecutionCondition> condition = make_shared<TaskExecutionCondition>(TaskExecutionCondition::BUFFER_MIGRATION,migratedBufferAddress);


        if(this->stageScheduler->getSchedulerType() != "NormalStageScheduler") {
            spdlog::error("MigrateBuffers: don't support migrating buffers for " + this->stageScheduler->getSchedulerType() + "!");
            return false;
        }

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(this->stageScheduler)->addConcurrentForInterTaskMission(condition));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();

        for(auto task : newTasks)
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());

        this->stageLinkage->processScheduleResultsToReplaceSourceTasks(newTasks,{taskId});

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
                }
        }
        if(originTaskPtrs.size() != taskIds.size())
            return false;

        TaskId taskIdTemp;
        MigratedBufferAddress migratedBufferAddress;

        for(auto originTask : originTaskPtrs) {
            migratedBufferAddress.addMigratedBufferAddress({ taskIdTemp.Serialize(*originTask->getTaskId()) }, {originTask->getIP()}, {"9081"}, {to_string(-1)});

        }

        shared_ptr<TaskExecutionCondition> condition = make_shared<TaskExecutionCondition>(TaskExecutionCondition::BUFFER_MIGRATION,migratedBufferAddress);


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
            this->monitoredTaskIds.push_back(task->getTaskId()->ToString());

        this->stageLinkage->processScheduleResultsToReplaceSourceTasks(newTasks,taskIds);

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
        spdlog::info("Sideway exchange schedule start!");

        if(this->missionType == OPERATOR_MIGRATION)
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

    bool schedule()
    {
        if(this->missionType == BUFFER_MIGRATION && this->dataExchangeMode == MANY_TO_MANY)
        {
            return migrateBuffersManyToMany(this->taskIds,this->targetTaskNumber);
        }

        for(auto taskId : this->taskIds)
            if(!schedule(taskId))
                return false;

        return true;
    }

    void releaseMonitor()
    {
        thread monitor(stateMonitor,this);
        monitor.detach();
    }
    static void stateMonitor(SidewayDataExchangeScheduler* scheduler)
    {
        while(1)
        {
            if(scheduler->isSchedulingFinished()) {
                scheduler->schedulingFinished = true;
                spdlog::info("Sideway exchange schedule finished!");
                break;
            }
            sleep_for(std::chrono::milliseconds(500));
        }
    }

    bool isSchedulingFinished()
    {
        for(auto task : this->monitoredTaskIds) {
            if(this->opsNeededToMigrate.contains("LookupJoinOperator")) {
                if (!this->sqlStageExecution->isTaskDependenciesSatisfied(task))
                    return false;
            }
            else{
                if (!this->sqlStageExecution->isTaskFinished(task))
                    return false;
            }
        }

        return true;
    }


};



#endif //OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP
