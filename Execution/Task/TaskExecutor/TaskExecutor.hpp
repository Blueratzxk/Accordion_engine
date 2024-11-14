//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_TASKEXECUTOR_HPP
#define OLVP_TASKEXECUTOR_HPP

#include "../../../Utils/ThreadPool.h"
#include "TaskExecutorRunner.hpp"
#include "../TaskHandle.hpp"
#include "tbb/concurrent_queue.h"
#include "../../../Utils/BlockQueue.hpp"


class TaskExecutor: public std::enable_shared_from_this<TaskExecutor>
{

    vector<std::shared_ptr<TaskHandle>> tasks;
    list<std::shared_ptr<TaskExecutorRunner>> allTaskExecutorRunners;


    BlockQueue<std::shared_ptr<TaskExecutorRunner>> runnerQueue;

    long runnerId = 0;




    class TaskWorkers
    {
        std::shared_ptr<TaskExecutor> taskExecutor;
        std::shared_ptr<ThreadPool> threadPool;
    public:
        TaskWorkers(std::shared_ptr<TaskExecutor> taskExecutor)
        {
            this->taskExecutor = taskExecutor;
            this->threadPool = std::make_shared<ThreadPool>();
            this->threadPool->start(100);
            spdlog::debug("ThreadPool initial finished!");
        }

        static void run(std::shared_ptr<TaskExecutorRunner> runner) {
            runner->process();
        }

        static void start(std::shared_ptr<TaskWorkers> taskWorkers){
            while (true)
            {
                shared_ptr<TaskExecutorRunner> runner = taskWorkers->taskExecutor->getRunner();
                if(runner != NULL)
                {
                    taskWorkers->threadPool->submitTask(run,runner);
                }
            }
        }


    };


    std::shared_ptr<TaskWorkers> taskWorkers ;


public:
    TaskExecutor()
    {

    }
    void start()
    {
        this->taskWorkers = make_shared<TaskWorkers>(shared_from_this());
        thread(TaskWorkers::start,(taskWorkers)).detach();

    }

    std::shared_ptr<TaskHandle> addTask(std::shared_ptr<TaskId> taskId)
    {
        auto taskHandle = std::make_shared<TaskHandle>(taskId);
        this->tasks.push_back(std::make_shared<TaskHandle>(taskId));
        return taskHandle;
    }


    void  enqueueSplits(std::shared_ptr<TaskHandle> taskHandle,vector<std::shared_ptr<SplitRunner>> splitRunners)
    {
        for(auto runner : splitRunners)
        {
            std::shared_ptr<TaskExecutorRunner> taskExecutorRunner = std::make_shared<TaskExecutorRunner>(runner,taskHandle);
            taskHandle->enqueueTaskExecutorRunner(taskExecutorRunner);
            spdlog::debug("Add Task!");
            this->startRunner(taskHandle->pollTaskExecutorRunner());
        }
    }

    void startRunner(std::shared_ptr<TaskExecutorRunner> runner)
    {
        allTaskExecutorRunners.push_back(runner);
        runnerQueue.Put(runner);
    }

    std::shared_ptr<TaskExecutorRunner> getRunner()
    {
        std::shared_ptr<TaskExecutorRunner> runner = this->runnerQueue.Take();
        return runner;
    }




};














#endif //OLVP_TASKEXECUTOR_HPP
