//
// Created by zxk on 12/16/24.
//

#ifndef OLVP_PROGRESSANDTUNER_HPP
#define OLVP_PROGRESSANDTUNER_HPP


class ProgressAndTuner
{
    int tableScanStageId;
    list<int> tuningKnobs;

    list<shared_ptr<ProgressAndTuner>> executionDependencies;

public:
    ProgressAndTuner(){

    }

    void setTableScanProgress(int tableScanStageId)
    {
        this->tableScanStageId = tableScanStageId;
    }

    void addTuningKnobStage(int knob)
    {
        this->tuningKnobs.push_back(knob);
    }

    void setExecutionDependencies(list<shared_ptr<ProgressAndTuner>> executionDependencies){
        this->executionDependencies = executionDependencies;
    }
};

#endif //OLVP_PROGRESSANDTUNER_HPP
