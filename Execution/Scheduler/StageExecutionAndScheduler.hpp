//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_STAGEEXECUTIONANDSCHEDULER_HPP
#define OLVP_STAGEEXECUTIONANDSCHEDULER_HPP


#include "SqlStageExecution.hpp"
#include "StageLinkage.hpp"
#include "StageScheduler.hpp"
class StageExecutionAndScheduler
{
    shared_ptr<SqlStageExecution> stageExecution;
    shared_ptr<StageLinkage> linkage;
    shared_ptr<StageScheduler>  scheduler;

public:
    StageExecutionAndScheduler(shared_ptr<SqlStageExecution> stageExecution,shared_ptr<StageLinkage> linkage,shared_ptr<StageScheduler> scheduler){
        this->stageExecution = stageExecution;
        this->linkage = linkage;
        this->scheduler = scheduler;
    }
    shared_ptr<SqlStageExecution> getStageExecution()
    {
        return this->stageExecution;
    }
    shared_ptr<StageLinkage> getStageLinkage()
    {
        return this->linkage;
    }
    shared_ptr<StageScheduler> getStageScheduler()
    {
        return this->scheduler;
    }

};


#endif //OLVP_STAGEEXECUTIONANDSCHEDULER_HPP
