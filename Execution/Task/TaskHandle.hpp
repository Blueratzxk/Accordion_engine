//
// Created by zxk on 6/2/23.
//

#ifndef OLVP_TASKHANDLE_HPP
#define OLVP_TASKHANDLE_HPP

#include "Id/TaskId.hpp"

class TaskExecutorRunner;
class TaskHandle
{
    std::shared_ptr<TaskId> taskId;
    std::queue<std::shared_ptr<TaskExecutorRunner>> taskExecutorRunners;
public:
    TaskHandle(std::shared_ptr<TaskId> taskId)
    {
        this->taskId = taskId;
    }

    void enqueueTaskExecutorRunner(std::shared_ptr<TaskExecutorRunner> runner)
    {
        this->taskExecutorRunners.push(runner);
    }

    std::shared_ptr<TaskExecutorRunner> pollTaskExecutorRunner()
    {
        std::shared_ptr<TaskExecutorRunner> runner = this->taskExecutorRunners.front();
        this->taskExecutorRunners.pop();
        return runner;
    }



};


#endif //OLVP_TASKHANDLE_HPP
