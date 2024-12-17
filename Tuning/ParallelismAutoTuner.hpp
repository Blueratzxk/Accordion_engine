//
// Created by zxk on 12/16/24.
//

#ifndef OLVP_PARALLELISMAUTOTUNER_HPP
#define OLVP_PARALLELISMAUTOTUNER_HPP


#include "../Execution/Scheduler/SqlQueryExecution.hpp"
#include "Prediction/PPM.hpp"
#include <string>
#include "../Execution/Scheduler/QueryInfos/StageProcessingTimeDict.hpp"
#include "../Utils/StringUtils.hpp"
using namespace std;


class ParallelismAutoTuner
{
    //shared_ptr<map<string,shared_ptr<SqlQueryExecution>>> queries;
    shared_ptr<PPM> ppm;
    shared_ptr<SqlQueryExecution> sqlQueryExecution = NULL;
    shared_ptr<ProgressAndTuner> pAt;
    shared_ptr<StageProcessingTimeDict> stageProcessingTimeDict;
    string queryName;
public:
    ParallelismAutoTuner(shared_ptr<PPM> ppm)
    {
        this->ppm = ppm;
        this->stageProcessingTimeDict = make_shared<StageProcessingTimeDict>();
    }

    void tune(string queryId)
    {
        sqlQueryExecution = this->ppm->getQueryExecution(queryId);
        if(sqlQueryExecution == NULL)
            return;
        pAt = sqlQueryExecution->getProgressAndTuner();
        vector<string> results;
        StringUtils::Stringsplit(queryId,'-',results);
        queryName = results[results.size()-2];

        if(!sqlQueryExecution->isQuerySilent()) {
            double queryTime = computeQueryExecutionTime(this->pAt);
            spdlog::info(queryTime);
        }
        else
            spdlog::info("Query is silent, cannot give prediction!");
    }

    double computeQueryExecutionTime(shared_ptr<ProgressAndTuner> pAt)
    {
        double time = 0;
        double maxDependTime = 0;
        auto stage = this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(pAt->getTableScanStageId());


        for(auto pat : pAt->getExecutionDependencies())
        {
            double dependTime;
            dependTime = computeQueryExecutionTime(pat);
            if(dependTime > maxDependTime)
                maxDependTime = dependTime;
        }
        time+=maxDependTime;


        bool dependencies = true;
        int knobDop = 1;
        for(auto stageId : pAt->getTuningKnobStages())
        {
            auto knob = this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(stageId);
            knobDop = knob->getStageExecution()->getCurrentStageDOP();
            if(!knob->getStageExecution()->isDependenciesSatisfied())
                dependencies = false;
        }

        if(!dependencies)
        {
            auto processingTime = this->stageProcessingTimeDict->getProcessingTime(queryName,stage->getStageExecution()->getStageId().getId(),knobDop);
            spdlog::info("Using history time:"+ to_string(processingTime));
            return processingTime+time;
        }
        else if(!stage->getStageExecution()->getState()->isDone() && stage->getStageExecution()->getState()->isStart()) {
            spdlog::info("Using prediction time:"+ to_string(stage->getStageExecution()->getRemainingTime()));
            return stage->getStageExecution()->getRemainingTime() + time;
        }
        else
            return 0.0;
    }


};
#endif //OLVP_PARALLELISMAUTOTUNER_HPP
