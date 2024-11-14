//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_TASKSRUNTIMESTATS_HPP
#define OLVP_TASKSRUNTIMESTATS_HPP

#include <atomic>
using namespace std;
class TasksRuntimeStats
{
    atomic<int> activeThreads = 0;
    atomic<int> activeTasks = 0;
public:
    TasksRuntimeStats()
    {
    }
    void increaseThreadNums()
    {
        this->activeThreads++;
    }
    void decreaseThreadNums()
    {
        this->activeThreads--;
    }

    void increaseTaskNums()
    {
        this->activeTasks++;
    }
    void decreaseTaskNums()
    {
        this->activeTasks--;
    }

    int getAllActiveThreadNums()
    {
        return this->activeThreads;
    }
    int getAllActiveTasks()
    {
        return this->activeTasks;
    }

};


#endif //OLVP_TASKSRUNTIMESTATS_HPP
