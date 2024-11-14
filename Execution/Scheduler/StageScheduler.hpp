//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_STAGESCHEDULER_HPP
#define OLVP_STAGESCHEDULER_HPP


#include "../Task/RemoteTask.hpp"
class ScheduleResult
{
    vector<shared_ptr<HttpRemoteTask>> newTasks;
public:
    ScheduleResult(vector<shared_ptr<HttpRemoteTask>> newTasks){
        this->newTasks = newTasks;
    }
    vector<shared_ptr<HttpRemoteTask>> getNewTasks()
    {
        return this->newTasks;
    }
    list<TaskId> getTaskIds()
    {
        list<TaskId> taskIds;
        for(auto task: newTasks)
        {
            taskIds.push_back(*task->getTaskId());
        }
        return taskIds;
    }

};


class StageScheduler
{
    string SchedulerType;
public:
    StageScheduler(string type){
        this->SchedulerType = type;
    }
    string getSchedulerType() {return this->SchedulerType;}

    virtual ScheduleResult schedule() = 0;
    virtual void close() = 0;
};


#endif //OLVP_STAGESCHEDULER_HPP
