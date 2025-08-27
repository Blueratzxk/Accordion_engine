//
// Created by zxk on 8/26/25.
//

#ifndef OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP
#define OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP


class SidewayDataExchangeScheduler
{
    shared_ptr<SqlStageExecution> sqlStageExecution;
    shared_ptr<StageScheduler> stageScheduler;
    shared_ptr<StageLinkage> stageLinkage;
    int taskId; //task needed to migrate

    map<string,string> migratableOperators = {
            {"FinalAggregationOperator","FinalAggregationNode"}
    };

    set<string> opsNeedInterTaskExchangeService
    {
            "FinalAggregationOperator"
    };
public:

    SidewayDataExchangeScheduler(shared_ptr<SqlStageExecution> sqlStageExecution,shared_ptr<StageScheduler> stageScheduler,shared_ptr<StageLinkage> stageLinkage,int taskId)
    {
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

        return sqlStageExecution->taskMigrationPreparation(taskId,opsNeededToMigrate);
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

        MigratedOperators migratedOperators(opsNeededToMigrate,needExchangeService,taskIdString,ip,port);

        ScheduleResult result = (static_pointer_cast<NormalStageScheduler>(stageScheduler))->addOneConcurrentForInterTaskMission(
                make_shared<TaskExecutionCondition>(TaskExecutionCondition::OPERATOR_MIGRATION,migratedOperators));
        vector<shared_ptr<HttpRemoteTask>> newTasks = result.getNewTasks();
        this->stageLinkage->processScheduleResultsToAddConcurrent(newTasks);
    }


    bool schedule()
    {
       if(migratedOperatorPreparation())
           addTasksWithConditionExecution();
    }
};



#endif //OLVP_SIDEWAYDATAEXCHANGESCHEDULER_HPP
