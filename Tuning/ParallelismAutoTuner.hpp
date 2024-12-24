//
// Created by zxk on 12/16/24.
//

#ifndef OLVP_PARALLELISMAUTOTUNER_HPP
#define OLVP_PARALLELISMAUTOTUNER_HPP


#include <string>
#include "../Execution/Scheduler/QueryInfos/StageProcessingTimeDict.hpp"
#include "../Utils/StringUtils.hpp"
#include "AutoTunePlanConfig.hpp"
using namespace std;

class AutoTuneInfo
{
public:
    enum TuneType {ADD,SUB} type;
private:
    int stageId;
    int degree;

    long long timeStamp;
public:
    AutoTuneInfo(int stageId, TuneType type, int degree, long long timeStamp)
    {
        this->stageId = stageId;
        this->type = type;
        this->degree = degree;
        this->timeStamp = timeStamp;
    }

    int getDegree()
    {
        return this->degree;
    }

    int getStageId(){
        return this->stageId;
    }

    long long getTimeStamp()
    {
        return this->timeStamp;
    }

};

class ParallelismAutoTuner
{
    //shared_ptr<map<string,shared_ptr<SqlQueryExecution>>> queries;
    shared_ptr<PPM> ppm;
    shared_ptr<SqlQueryExecution> sqlQueryExecution = NULL;
    shared_ptr<ProgressAndTuner> pAt;
    shared_ptr<StageProcessingTimeDict> stageProcessingTimeDict;
    string queryName;

    double fluctuation = 10000.0;//ms
    int tryCount = 10;

    map<int,long long> stageTuningTime;
    map<int,long> stageElasTime;

    bool halt = false;
    bool monitorMode = false;

    list<shared_ptr<AutoTuneInfo>> autoTuneLogs;

public:
    ParallelismAutoTuner(shared_ptr<PPM> ppm)
    {
        this->ppm = ppm;
        this->stageProcessingTimeDict = make_shared<StageProcessingTimeDict>();
    }

    void haltTuner()
    {
        halt = true;
    }

    bool isMonitorMode()
    {
        return this->monitorMode;
    }

    void turnOnStageDOP(shared_ptr<SqlQueryExecution> sqlQueryExecution,int stageId,int degree)
    {

        this->autoTuneLogs.push_back(make_shared<AutoTuneInfo>(stageId,AutoTuneInfo::ADD,degree,TimeCommon::getCurrentTimeStamp()));
        if(!sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution()->isDOPSwitchingType())
            sqlQueryExecution->Dynamic_addStageTaskGroupConcurrent(stageId,degree);
        else
            sqlQueryExecution->Dynamic_addStageTaskGroupConcurrent(stageId,degree);
    }

    void turnDownStageDOP(shared_ptr<SqlQueryExecution> sqlQueryExecution,int stageId,int degree)
    {
        this->autoTuneLogs.push_back(make_shared<AutoTuneInfo>(stageId,AutoTuneInfo::SUB,degree,TimeCommon::getCurrentTimeStamp()));
        if(!sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution()->isDOPSwitchingType())
            sqlQueryExecution->Dynamic_decreaseStageParallelism(stageId,degree);
        else
            sqlQueryExecution->Dynamic_addStageTaskGroupConcurrent(stageId,degree);
    }

    double estimateQueryExecutionTimeByHistory(string queryId,bool &success)
    {
        double queryTime = 0.0;


        if(!sqlQueryExecution->isQuerySilent()) {
            queryTime = computeQueryExecutionTime(queryId,this->pAt,success);
            if(!success)
                spdlog::info("Cannot get steady stage prediction execution time!");
            else
                spdlog::info(queryTime);
        }
        else {
            success = false;
            spdlog::info("Query is silent, cannot give prediction!");
        }
        return queryTime;
    }

    void tuneToTargetDOP(shared_ptr<SqlQueryExecution> sqlQueryExecution,int stageId,int targetDOP)
    {
        this->stageTuningTime[stageId] = TimeCommon::getCurrentTimeStamp();
        int curDOP = this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(stageId)->getStageExecution()->getCurrentStageDOP();

        if(curDOP > targetDOP)
            turnDownStageDOP(sqlQueryExecution,stageId,curDOP-targetDOP);
        else if(curDOP < targetDOP)
            turnOnStageDOP(sqlQueryExecution,stageId,targetDOP-curDOP);
    }

    int findDopByTimeConstraint(map<double,double> DOPToRemainingTime,string timeConstraint)
    {
        double constraint = atof(timeConstraint.c_str());
        for(auto dt : DOPToRemainingTime)
        {
            if(constraint > dt.second) {
                int dopFound = (int) dt.first;
                if(dopFound != 0)
                    return dopFound;
                else
                    return -1;
            }
        }
        return -1;
    }

    list<shared_ptr<AutoTuneInfo>> getAutoTuneInfos()
    {
        return this->autoTuneLogs;
    }

    void dopMonitor(string queryId)
    {
        this->monitorMode = true;
        vector<string> results;
        StringUtils::Stringsplit(queryId,'-',results);
        queryName = results[results.size()-2];
        sqlQueryExecution = this->ppm->getQueryExecution(queryId);
        if(sqlQueryExecution == NULL || !sqlQueryExecution->isQueryStart())
            return;

        //autoTuneByPlan(queryId);


        while(!sqlQueryExecution->isQueryFinished())
        {
            if(halt)
                break;

            long long tuneBefore = TimeCommon::getCurrentTimeStamp();
            autoTuneByPlan(queryId);
            usleep(1000000);
            long long tuneAfter = TimeCommon::getCurrentTimeStamp();

            for(auto &constraint : this->stageElasTime) {
                if(constraint.second != -1)
                    constraint.second -= ((tuneAfter - tuneBefore)/1000);
            }

        }


    }

    bool autoTuneByPlan(string queryId)
    {

        shared_ptr<AutoTunePlanConfig> autoTunePlanConfig = make_shared<AutoTunePlanConfig>();

        this->pAt = sqlQueryExecution->getProgressAndTuner();
        bool success = true;
        double time = estimateQueryExecutionTimeByHistory(queryId,success);
        map<int,int> stageToTunings;

        if(success) {

            auto bottlenecks = this->ppm->getQueryBottleneckStages(queryId);

            vector<double> targetDOPs = {1,2,3,4,5,6,7,8,9,10};
            vector<double> factors;



            for(auto bottleneck: bottlenecks)
            {
                auto stageObj = this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(bottleneck)->getStageExecution();
                if(!stageObj->isDependenciesSatisfied())
                    continue;
                if(stageObj->getFragment()->hasTableScan())
                    continue;
                if(!stageObj->isDependenciesSatisfied())
                    continue;
                if(stageObj->getLastStageMigratingTimeStamp() != -1 && TimeCommon::getCurrentTimeStamp() - stageObj->getLastStageMigratingTimeStamp() < 5000)
                    continue;

                if(this->stageTuningTime.contains(bottleneck) && TimeCommon::getCurrentTimeStamp() - this->stageTuningTime[bottleneck] < 5000)
                    continue;


                int curDOP = this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(bottleneck)->getStageExecution()->getCurrentStageDOP();
                for(int i = 0 ; i < targetDOPs.size() ; i++)
                    factors.push_back(targetDOPs[i]/curDOP);

                auto res = this->ppm->getStageImprovementPredictionInfosList(queryId,bottleneck,curDOP,factors);
                for(auto re : res)
                    spdlog::info(to_string(re.first)+"|"+ to_string(re.second));

                if(!this->stageElasTime.contains(bottleneck)) {
                    auto timeConstraint = autoTunePlanConfig->getTimeConstraint(queryName, to_string(
                            this->findTableScanIdByBottleneck(this->pAt, bottleneck)));
                    this->stageElasTime[bottleneck] = timeConstraint;
                }
                long buildTime = this->stageProcessingTimeDict->getBuildTime(queryName,this->findTableScanIdByBottleneck(this->pAt, bottleneck),curDOP);
                long curProcessingTime = this->sqlQueryExecution->getScheduler()->getCurProcessingTime(this->findTableScanIdByBottleneck(this->pAt, bottleneck));
                if(buildTime == -1) {
                    spdlog::info("Lack of build time! Auto tuning failure!");
                    return false;
                }
                long accuracyTimeConstraint = this->stageElasTime[bottleneck] - (buildTime/1000);
                spdlog::info("Accuracy time constraint is" + to_string(accuracyTimeConstraint));

                if(this->stageElasTime[bottleneck] == -1) {
                    spdlog::info("Lack of timeConstraint! Auto tuning failure!");
                    return false;
                }

                int dopFound = findDopByTimeConstraint(res, to_string(accuracyTimeConstraint));
                if(dopFound == -1) {
                    spdlog::info("Cannot find appropriate DOP!");
                    dopFound = 8;
                }
                else {
                    spdlog::info(
                            "DOP" + to_string(dopFound) + " can satisfy the time constraint! " + "Current DOP is " +
                            to_string(curDOP));

                        int tableScanId = this->findTableScanIdByBottleneck(this->pAt, bottleneck);
                        auto knobs = this->findTuningKnobsByTableScanId(this->pAt, tableScanId);
                        for (int knob: knobs)
                            stageToTunings[knob] = dopFound;
                }
            }

            if(bottlenecks.empty())
                spdlog::info("No bottleneck stages detected!");

            for(auto stage : stageToTunings)
                this->tuneToTargetDOP(sqlQueryExecution,stage.first,stage.second);
            //tuneEachKnob(this->pAt);
        }

        return !stageToTunings.empty();
    }

    void tune(string queryId,string timeConstraint)
    {
        vector<string> results;
        StringUtils::Stringsplit(queryId,'-',results);
        queryName = results[results.size()-2];

        sqlQueryExecution = this->ppm->getQueryExecution(queryId);
        if(sqlQueryExecution == NULL)
            return;
        this->pAt = sqlQueryExecution->getProgressAndTuner();
        bool success = true;
        double time = estimateQueryExecutionTimeByHistory(queryId,success);


        if(success) {

            auto bottlenecks = this->ppm->getQueryBottleneckStages(queryId);

            vector<double> targetDOPs = {1,2,3,4,5,6,7,8,9,10};
            vector<double> factors;

            map<int,int> stageToTunings;

            for(auto bottleneck: bottlenecks)
            {
                int curDOP = this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(bottleneck)->getStageExecution()->getCurrentStageDOP();
                for(int i = 0 ; i < targetDOPs.size() ; i++)
                    factors.push_back(targetDOPs[i]/curDOP);

                auto res = this->ppm->getStageImprovementPredictionInfosList(queryId,bottleneck,curDOP,factors);
                for(auto re : res)
                    spdlog::info(to_string(re.first)+"|"+ to_string(re.second));


                int dopFound = findDopByTimeConstraint(res,timeConstraint);
                if(dopFound == -1)
                    spdlog::info("Cannot find appropriate DOP!");
                else {
                    spdlog::info(
                            "DOP" + to_string(dopFound) + " can satisfy the time constraint! " + "Current DOP is " +
                            to_string(curDOP));

                    if(!this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(bottleneck)->getStageExecution()->getFragment()->hasTableScan()) {
                        int tableScanId = this->findTableScanIdByBottleneck(this->pAt, bottleneck);
                        auto knobs = this->findTuningKnobsByTableScanId(this->pAt, tableScanId);
                        for (int knob: knobs)
                            stageToTunings[knob] = dopFound;
                    }
                    else
                        spdlog::info("Stage "+ to_string(bottleneck)+" is data source bottleneck!");
                }
            }

            if(bottlenecks.empty())
                spdlog::info("No bottleneck stages detected!");

            for(auto stage : stageToTunings)
                this->tuneToTargetDOP(sqlQueryExecution,stage.first,stage.second);
            //tuneEachKnob(this->pAt);
        }
    }

    int findTableScanIdByBottleneck(shared_ptr<ProgressAndTuner> pAt, int bottleneck)
    {

        auto tuningKnobs = pAt->getTuningKnobStages();
        for(auto knob : tuningKnobs)
        {
            if(knob == bottleneck)
                return pAt->getTableScanStageId();
        }

        for(auto pat : pAt->getExecutionDependencies()) {
            auto re = findTableScanIdByBottleneck(pat, bottleneck);
            if(re != -1)
                return re;
        }
        return -1;
    }

    list<int> findTuningKnobsByTableScanId(shared_ptr<ProgressAndTuner> pAt, int tableScanId)
    {
        if(pAt->getTableScanStageId() == tableScanId)
            return pAt->getTuningKnobStages();

        for(auto pat : pAt->getExecutionDependencies()) {
            auto re = findTuningKnobsByTableScanId(pat, tableScanId);
            if(!re.empty())
                return re;
        }
        return {};
    }

    void tuneEachKnob(shared_ptr<ProgressAndTuner> pAt)
    {


        for(auto pat : pAt->getExecutionDependencies())
            tuneEachKnob(pat);

        if(this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(pAt->getTableScanStageId())->getStageExecution()->getRemainingTime() > 0) {
            for (auto knob: pAt->getTuningKnobStages()) {
                this->turnOnStageDOP(this->sqlQueryExecution, knob, 1);
            }
        }

    }

    double getSteadyEstimatedStageExecutionTime(string queryId,shared_ptr<SqlStageExecution> stage)
    {
        int tryCounter = tryCount;
        double timeAlpha = 0.0;
        double timeBeta = 0.0;
        do {
            timeAlpha = stage->getRemainingTime();
            usleep(1000000);
            timeBeta = stage->getRemainingTime();
            tryCounter--;
            if(tryCounter <= 0)
                return -1;

        }while(timeAlpha == timeBeta || fabs(timeBeta - timeAlpha) > fluctuation);

        return timeBeta;

    }

    double getSteadyEstimatedExecutionTimeForStage(string queryId,shared_ptr<SqlStageExecution> stage)
    {
        int tryCounter = tryCount;
        double curfluctuation = 0.0;
        do {
            usleep(200000);
            if(stage->getState()->isDone())
                break;
            curfluctuation  = this->ppm->getRemainingTimeFluctuationOfQueryStage(queryId,stage->getStageId().getId());
        }while(curfluctuation <= 0 || curfluctuation > 0.5);

        return this->ppm->getAvgRemainingTimeOfQueryStage(queryId,stage->getStageId().getId());
    }

    double computeQueryExecutionTime(string queryId,shared_ptr<ProgressAndTuner> pAt,bool &success)
    {
        double time = 0;
        double maxDependTime = 0;
        auto stage = this->sqlQueryExecution->getScheduler()->getStageExecutionAndSchedulerByStagId(pAt->getTableScanStageId());


        for(auto pat : pAt->getExecutionDependencies())
        {
            double dependTime;
            dependTime = computeQueryExecutionTime(queryId,pat,success);
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
            if(processingTime == -1)
            {
                processingTime = 0;
                spdlog::warn("Lack of history information of "+queryName+"-"+to_string(stage->getStageExecution()->getStageId().getId())+"-"+
                                                                          to_string(knobDop)+". Prediction is inaccurate!");
            }
            else
                spdlog::info("Using history time:"+ to_string(processingTime));
            return processingTime+time;
        }
        else if(!stage->getStageExecution()->getState()->isDone() && stage->getStageExecution()->getState()->isStart()) {

            double prediction = getSteadyEstimatedExecutionTimeForStage(queryId,stage->getStageExecution());
            if(prediction > 0) {
                spdlog::info("Using prediction time:" + to_string(stage->getStageExecution()->getRemainingTime()));
                return prediction + time;
            }
            else
            {
                success = false;
                return -1;
            }
        }
        else
            return 0.0;
    }


};
#endif //OLVP_PARALLELISMAUTOTUNER_HPP
