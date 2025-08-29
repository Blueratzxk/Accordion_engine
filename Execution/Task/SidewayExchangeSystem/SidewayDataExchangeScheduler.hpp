//
// Created by zxk on 8/26/25.
//

#ifndef OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP
#define OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP


class SidewayDataExchangeScheduler
{

public:
    enum MissionType {
        OPERATOR_MIGRATION,
        BUFFER_MIGRATION,
    };

private:
    MissionType missionType;
    shared_ptr<SqlStageExecution> sqlStageExecution;
    shared_ptr<StageScheduler> stageScheduler;
    shared_ptr<StageLinkage> stageLinkage;
    int taskId; //task needed to migrate

    map<string,string> migratableOperators = {
            {"FinalAggregationOperator","FinalAggregationNode"},
            {"LookupJoinOperator","LookupJoinNode"}
    };

    set<string> opsNeedInterTaskExchangeService
    {
            "FinalAggregationOperator"
    };

    map<string,set<string>> sourceIdMap;

    list<int> taskIdsToMigrateBuffers;
public:

    SidewayDataExchangeScheduler(MissionType type,shared_ptr<SqlStageExecution> sqlStageExecution,shared_ptr<StageScheduler> stageScheduler,shared_ptr<StageLinkage> stageLinkage,int taskId)
    {
        this->missionType = type;
        this->sqlStageExecution = sqlStageExecution;
        this->stageScheduler = stageScheduler;
        this->stageLinkage = stageLinkage;
        this->taskId = taskId;
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

    bool migratedOperatorPreparation()
    {

        auto opsNeededToMigrate = analyzeTaskMigratableOperators();
        if(opsNeededToMigrate.empty())
            return false;

        auto result = sqlStageExecution->taskMigrationPreparation(taskId,opsNeededToMigrate);
        this->sourceIdMap = result->getSourceIdMap();
        return result->getStatus();
    }

    void addTasksWithConditionExecution()
    {

        auto opsNeededToMigrate = analyzeTaskMigratableOperators();
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

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(stageScheduler))->addOneConcurrentForInterTaskMission(
                make_shared<TaskExecutionCondition>(TaskExecutionCondition::OPERATOR_MIGRATION,migratedOperators));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();
        this->stageLinkage->processScheduleResultsToAddConcurrent(newTasks);
    }

    bool migrateOperators()
    {
        if(migratedOperatorPreparation()) {
            addTasksWithConditionExecution();
            return true;
        }
        return false;
    }

    bool migrateBuffers() {

        vector<shared_ptr<HttpRemoteTask>> tasks = sqlStageExecution->getAllTasks();
        shared_ptr<HttpRemoteTask> oldTaskPtr = NULL;
        for (auto task: tasks)
            if (task->getTaskId()->getId() == taskId) {
                oldTaskPtr = task;
            }
        if(oldTaskPtr == NULL)
            return false;

        TaskId taskIdTemp;
        MigratedBufferAddress migratedBufferAddress(taskIdTemp.Serialize(*oldTaskPtr->getTaskId()),oldTaskPtr->getIP(),"9081","-1");
        shared_ptr<TaskExecutionCondition> condition = make_shared<TaskExecutionCondition>(TaskExecutionCondition::BUFFER_MIGRATION,migratedBufferAddress);


        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(this->stageScheduler)->addOneConcurrentForInterTaskMission(condition));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();
        this->stageLinkage->processScheduleResultsToReplaceSourceTasks(newTasks,{taskId});

        return true;
    }

    bool schedule()
    {
        if(this->missionType == OPERATOR_MIGRATION)
            return migrateOperators();
        else if(this->missionType == BUFFER_MIGRATION)
            return migrateBuffers();
        else {
            spdlog::info("Unsupported sideway exchange type!");
            return false;
        }
    }
};



#endif //OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP
