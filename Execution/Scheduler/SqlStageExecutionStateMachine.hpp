//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SQLSTAGEEXECUTIONSTATEMACHINE_HPP
#define OLVP_SQLSTAGEEXECUTIONSTATEMACHINE_HPP


#include <atomic>
//#include "../../common.h"
#include "../Event/SimpleEvent.hpp"
class StageExecutionStateMachine
{
public:
    enum StageExecutionState {

        PLANNED,
        SCHEDULING,
        FINISHED_TASK_SCHEDULING,
        SCHEDULING_SPLITS,
        SCHEDULED,
        RUNNING,
        FINISHED,
        CANCELED,
        ABORTED,
        FAILED
    };
private:
    std::atomic<StageExecutionState>  state;


    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> startTime;
    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> endTime;

    shared_ptr<Event> eventListener;

public:
    StageExecutionStateMachine(){
        this->eventListener = make_shared<SimpleEvent>();
    }
    StageExecutionState getState()
    {
        return this->state;
    }

    shared_ptr<Event> getStateChangeListener(){
        return this->eventListener;
    }
    string getStateToString()
    {
        switch(this->state)
        {
            case PLANNED:
                return "PLANNED";
            case RUNNING:
                return "RUNNING";
            case FINISHED:
                return "FINISHED";
        }
        return "UNKNOWN";
    }

    void start(){
        this->startTime = std::chrono::steady_clock::now();
        this->state = RUNNING;
        this->eventListener->notify();
    }

    void finished(){
        this->endTime = std::chrono::steady_clock::now();
        this->state = FINISHED;
        this->eventListener->notify();
    }

    double getCurrentExecutionTime()
    {
        auto now = std::chrono::steady_clock::now();
        auto startTime = this->getStartTime();
        return std::chrono::duration<double, std::milli>(now - startTime).count();
    }
    bool isStart()
    {
        return this->state == RUNNING?true:false;
    }
    bool isDone()
    {
        return this->state == FINISHED?true:false;
    }

    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> getStartTime()
    {
        return this->startTime;
    }

    std::chrono::time_point<std::chrono::steady_clock, std::chrono::duration<double, std::milli>> getEndTime()
    {
        return this->endTime;
    }

    double getRunningTime()
    {
        if(!this->isDone() && this->getState() == StageExecutionState::RUNNING)
        {
            auto now = std::chrono::steady_clock::now();
            auto startTime = this->getStartTime();

            return std::chrono::duration<double, std::milli>(now - startTime).count();
        }
        else if(this->isDone())
        {
            return std::chrono::duration<double, std::milli>(this->getEndTime() - this->getStartTime()).count();
        }
        else
            return 0;
    }



};


#endif //OLVP_SQLSTAGEEXECUTIONSTATEMACHINE_HPP
