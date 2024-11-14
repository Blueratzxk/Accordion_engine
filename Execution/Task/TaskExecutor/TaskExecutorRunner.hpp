//
// Created by zxk on 6/2/23.
//

#ifndef OLVP_TASKEXECUTORRUNNER_HPP
#define OLVP_TASKEXECUTORRUNNER_HPP

#include "../SplitRunner.hpp"
#include "../TaskHandle.hpp"
#include "../../../common.h"

class TaskExecutorRunner
{
    std::shared_ptr<SplitRunner> splitRunner;
    std::shared_ptr<TaskHandle> belonged_TaskHandle;

public:
    TaskExecutorRunner( std::shared_ptr<SplitRunner> splitRunner,std::shared_ptr<TaskHandle> belonged_TaskHandle)
    {
        this->splitRunner = splitRunner;
        this->belonged_TaskHandle = belonged_TaskHandle;
    }
    std::shared_ptr<TaskHandle> getTaskHandle()
    {
        return this->belonged_TaskHandle;
    }
    void process()
    {
        this->splitRunner->ProcessFor();
    }


};


#endif //OLVP_TASKEXECUTORRUNNER_HPP
