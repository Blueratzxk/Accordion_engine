//
// Created by zxk on 12/12/24.
//

#ifndef OLVP_STAGEPROCESSINGTIMECOLLECTOR_HPP
#define OLVP_STAGEPROCESSINGTIMECOLLECTOR_HPP

#include "../SqlStageExecution.hpp"
class StageProcessingTimeCollector : public enable_shared_from_this<StageProcessingTimeCollector>
{

    int delay = 100;
    list<shared_ptr<SqlStageExecution>> sqlStageExecutions;
    map<shared_ptr<SqlStageExecution>,long> stageThroughputCounters;

    mutex lock;
public:

    StageProcessingTimeCollector(list<shared_ptr<SqlStageExecution>> sqlStageExecutions)
    {
        this->sqlStageExecutions = sqlStageExecutions;
    }


    void start(){

        thread process([](shared_ptr<StageProcessingTimeCollector> collector) {
            while (true) {
                sleep_for(std::chrono::milliseconds(collector->delay));

                collector->lock.lock();
                for (auto stage: collector->sqlStageExecutions) {
                    if (stage->stageHasThroughput() > 0) {
                        if (!collector->stageThroughputCounters.contains(stage))
                            collector->stageThroughputCounters[stage] = 1;
                        else
                            collector->stageThroughputCounters[stage]++;
                    }

                }

                for (auto it = collector->sqlStageExecutions.begin(); it != collector->sqlStageExecutions.end();) {
                    if ((*it)->getState() != NULL && (*it)->getState()->isDone()) {
                        collector->sqlStageExecutions.erase(it++);
                    } else {
                        it++;
                    }
                }
                collector->lock.unlock();

                if (collector->sqlStageExecutions.empty())
                    return;
            }
        },shared_from_this());
        process.detach();
    }

    map<shared_ptr<SqlStageExecution>,long> getStageProcessingTimes()
    {
        map<shared_ptr<SqlStageExecution>,long> results;

        lock.lock();
        for(auto counter : stageThroughputCounters)
        {
            results[counter.first] = counter.second * this->delay;
        }
        lock.unlock();

        return results;

    }





};


#endif //OLVP_STAGEPROCESSINGTIMECOLLECTOR_HPP
