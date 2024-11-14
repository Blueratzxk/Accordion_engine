//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_TASKSTATEMACHINE_HPP
#define OLVP_TASKSTATEMACHINE_HPP


#include <iostream>
#include <atomic>
#include "../StateMachine.hpp"
#include <chrono>
class TaskStateMachine
{

public:

    enum TaskState {
        PLANNED,
        RUNNING,
        FINISHED,
        CANCELED,
        ABORTED,
        FAILED
    };

private:
    StateMachine stateMachine;
    std::atomic<TaskState> state;

    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> startTime;
    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> endTime;

    atomic<bool> startTimeSeted;
    mutex lock;
public:

    TaskStateMachine()
    {
        this->startTimeSeted = false;
        this->planned();

    }

    TaskState getState()
    {
        return this->state;
    }

    void finished()
    {
        this->endTime = std::chrono::steady_clock::now();
        this->state = TaskState::FINISHED;

    }
    bool isRunning()
    {
        return this->state == TaskState::RUNNING ? true : false;
    }
    void planned()
    {
        this->state = TaskState::PLANNED;
    }
    void start()
    {
        if(!this->startTimeSeted) {

            auto time = std::chrono::steady_clock::now();
            this->startTime = time;
            this->startTimeSeted = true;
        }
        else
        {
            lock.lock();
            auto time = std::chrono::steady_clock::now();
            if(time < this->startTime)
                this->startTime = time;
            lock.unlock();
        }
        this->state = TaskState::RUNNING;
    }
    bool isFinished()
    {
        return this->state == TaskState::FINISHED? true: false;
    }

    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> getStartTime()
    {
        return this->startTime;
    }

    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> getEndTime()
    {
        return this->endTime;
    }


};




#endif //OLVP_TASKSTATEMACHINE_HPP
