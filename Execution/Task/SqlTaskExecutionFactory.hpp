//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_SQLTASKEXECUTIONFACTORY_HPP
#define OLVP_SQLTASKEXECUTIONFACTORY_HPP

#include "SqlTaskExecution.hpp"
#include "../../Planner/LocalPlanner/LogicalPipelineFactory.hpp"
#include "../../Planner/LocalPlanner/LocalPlanTreeAnalyzer.hpp"
#include "Context/QueryContext.h"

#include "../../Planner/Fragment.hpp"
#include "../Buffer/LazyOutputBuffer.hpp"

class SqlTaskExecutionFactory
{

    std::shared_ptr<LocalExecutionPlanner> localExecutionPlanner;
    std::shared_ptr<TaskExecutor> taskExecutor;


    std::shared_ptr<QueryContext> queryContext;




    vector<LogicalPipeline> lpipelines;
    map<string,LogicalPipeline*> ID_to_logicalPipeline;
    std::shared_ptr<SqlTaskExecution> sqlTaskExecution = NULL;
    shared_ptr<TasksRuntimeStats> tasksRuntimeStats;

public:
    SqlTaskExecutionFactory(shared_ptr<TasksRuntimeStats> tasksRuntimeStats,std::shared_ptr<QueryContext> queryContext,std::shared_ptr<LocalExecutionPlanner> localExecutionPlanner,std::shared_ptr<TaskExecutor> taskExecutor){
        this->localExecutionPlanner = localExecutionPlanner;
        this->taskExecutor = taskExecutor;
        this->queryContext = queryContext;
        this->tasksRuntimeStats = tasksRuntimeStats;


    }

    std::shared_ptr<SqlTaskExecution> createRuntimeMachine(shared_ptr<Session> session,shared_ptr<TaskId>taskId,std::shared_ptr<TaskStateMachine> taskStateMachine,
                                                           std::shared_ptr<PlanFragment> planFragment,shared_ptr<OutputBuffer> outputBuffer)
    {

        this->localExecutionPlanner = make_shared<LocalExecutionPlanner>(planFragment->getRoot(),outputBuffer);
        auto localExecutionPlan = this->localExecutionPlanner->plan();
        int joinNum = this->localExecutionPlanner->getContext()->getJoinNum();

        list<LogicalPipeline> ls = localExecutionPlan->getDriverFactories();
        for(auto pipeline : ls)
            this->lpipelines.push_back(pipeline);

        shared_ptr<TaskContext> taskContext = make_shared<TaskContext>(this->tasksRuntimeStats,this->queryContext,taskId,taskStateMachine,outputBuffer);

        taskContext->setJoinNum(joinNum);

        this->queryContext->addTaskContext((*taskId).getQueryId().getId(),(*taskId).ToString(),taskContext);

        this->sqlTaskExecution = make_shared<SqlTaskExecution>(session,taskId,taskStateMachine,taskContext,outputBuffer,this->taskExecutor);
        this->createPattern();
        this->sqlTaskExecution->createLogicalRunnerFactories();
        this->sqlTaskExecution->startTask();



        lpipelines.clear();
        ID_to_logicalPipeline.clear();

        return this->sqlTaskExecution;

    }





    void createPattern()
    {

        for(int i = 0 ; i < lpipelines.size(); i++)
        {

            ID_to_logicalPipeline[lpipelines[i].getPipelineId()] = &lpipelines[i];//首先记录一下每个pipeline的id和pipeline的对应
        }
        renamePipelineIds();

        for(int i = 0 ; i < lpipelines.size(); i++){//下面就是要扫描pipeline，分析pipeline的类型，每个pipeline都需要进行记录。
            // 需要根据pipeline类型来进行reg，并根据类型来记录关系


            if(isTableScanPipeline(lpipelines[i]))
                RegTableScanPipeline(lpipelines[i],1);
            else if(isSourcePipeline(lpipelines[i]))
                RegSourcePipeline(lpipelines[i],1);
            else if(isRemoteSourcePipeline(lpipelines[i]))
                RegRemoteSourcePipeline(lpipelines[i],1);

            RegLocalExchange(lpipelines[i]);
            RegJoinBridge(lpipelines[i]);

            if(isOutputPipeline(lpipelines[i]))
                RegOutputPipeline(lpipelines[i]);
        }
    }

    void renamePipelineIds()
    {
        for(int i = 0 ; i < lpipelines.size(); i++)
        {
            LogicalPipeline &lp = lpipelines[i];
            if(isTableScanPipeline(lp))
                lp.renamePipelineId("TableScan_CPU_"+lp.getPipelineId());
            else if(isSourcePipeline(lp))
                lp.renamePipelineId("Source_CPU_"+lp.getPipelineId());
        }
    }


    bool isTableScanPipeline(LogicalPipeline lp)
    {
        vector<int> types = lp.getPipelineTypes();
        for(int i = 0 ; i < types.size() ; i++)
        {
            if(types[i] == PIPELINE_TYPE_TABLESCAN)
                return true;
        }
        return false;
    }

    bool isRemoteSourcePipeline(LogicalPipeline lp)
    {
        vector<int> types = lp.getPipelineTypes();
        for(int i = 0 ; i < types.size() ; i++)
        {
            if(types[i] == PIPELINE_TYPE_REMOTESOURCE)
                return true;
        }
        return false;
    }


    bool isSinkPipeline(LogicalPipeline lp)
    {
        vector<int> types = lp.getPipelineTypes();
        for(int i = 0 ; i < types.size() ; i++)
        {
            if(types[i] == PIPELINE_TYPE_SINK)
                return true;
        }
        return false;
    }
    bool isOutputPipeline(LogicalPipeline lp)
    {
        vector<int> types = lp.getPipelineTypes();
        for(int i = 0 ; i < types.size() ; i++)
        {
            if(types[i] == PIPELINE_TYPE_OUTPUT)
                return true;
        }
        return false;
    }
    void RegOutputPipeline(LogicalPipeline lp)
    {
        this->sqlTaskExecution->RegOutputPipeline(lp.getPipelineId());
    }

    void RegTableScanPipeline(LogicalPipeline lp,int initial_concurrent)
    {
        this->sqlTaskExecution->RegATableScanLogicalPipeline(lp.getPipelineId(),lp.getNewLogicalPipeline(lp),lp.getPipelineTypes(),initial_concurrent);
        if(lp.hasSourceId())
            this->sqlTaskExecution->Reg_SourcePlanNodeId_To_LPipeline(lp.getSourceId(),lp.getPipelineId());

    }

    void RegSourcePipeline(LogicalPipeline lp,int initial_concurrent)
    {
        this->sqlTaskExecution->RegSourceLogicalPipeline(lp.getPipelineId(),lp.getNewLogicalPipeline(lp),lp.getPipelineTypes(),initial_concurrent);
        if(lp.hasSourceId())
            this->sqlTaskExecution->Reg_SourcePlanNodeId_To_LPipeline(lp.getSourceId(),lp.getPipelineId());

    }


    void RegRemoteSourcePipeline(LogicalPipeline lp,int initial_concurrent)
    {
        this->sqlTaskExecution->RegRemoteSourceLogicalPipeline(lp.getPipelineId(),lp.getNewLogicalPipeline(lp),lp.getPipelineTypes(),initial_concurrent);
        if(lp.hasSourceId())
            this->sqlTaskExecution->Reg_SourcePlanNodeId_To_LPipeline(lp.getSourceId(),lp.getPipelineId());

    }


    void RegLocalExchange(LogicalPipeline lp)
    {
        if(lp.hasSourceId())
            this->sqlTaskExecution->Reg_SourcePlanNodeId_To_LPipeline(lp.getSourceId(),lp.getPipelineId());
        for(int i = 0 ; i < lp.getLogicalPipelines().size() ; i++)
        {
            if(lp.getLogicalPipelines()[i]->getTypeId().compare("Logical_LocalExchangeSourceOperator") == 0)
            {
                auto source = static_pointer_cast<Logical_LocalExchangeSourceOperator>(lp.getLogicalPipelines()[i]);

                this->sqlTaskExecution->RegLocalExchange(lp.getPipelineId(),source->getLocalExchangeFactory());
            }
        }
    }
    void RegJoinBridge(LogicalPipeline lp)
    {
        for(int i = 0 ; i < lp.getLogicalPipelines().size() ; i++) {
            if(lp.getLogicalPipelines()[i]->getTypeId().compare("Logical_LookupJoinOperator") == 0)
            {
                auto hashJoin = static_pointer_cast<Logical_LookupJoinOperator>(lp.getLogicalPipelines()[i]);

                this->sqlTaskExecution->RegCPUJoinBridge("joinBridge",hashJoin->getLookupSourceFactory());
            }
        }

    }
    bool isSourcePipeline(LogicalPipeline lp)
    {
        vector<int> types = lp.getPipelineTypes();
        for(int i = 0 ; i < types.size() ; i++)
        {
            if(types[i] == PIPELINE_TYPE_SOURCE)
                return true;
        }
        return false;
    }






};

#endif //OLVP_SQLTASKEXECUTIONFACTORY_HPP
