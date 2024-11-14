//
// Created by zxk on 6/12/23.
//

#ifndef OLVP_QUERYSTATEMACHINE_HPP
#define OLVP_QUERYSTATEMACHINE_HPP


#include <iostream>
#include <atomic>
#include "../Execution/StateMachine.hpp"
class QueryStateMachine
{

public:

    enum QueryState {
        PLANNED,
        RUNNING,
        FINISHED,
        CANCELED,
        ABORTED,
        FAILED,
        CANIQRS
    };

private:
    StateMachine stateMachine;
    std::atomic<QueryState> state;
public:

    QueryState getState()
    {
        return this->state;
    }

    void finished()
    {
        this->state = QueryState::FINISHED;

    }

    void canceled()
    {
        this->state = QueryState::CANCELED;
    }
    void openIQRS()
    {
        this->state = QueryState::CANIQRS;
    }
    void planned()
    {
        this->state = QueryState::PLANNED;
    }
    void start()
    {
        this->state = QueryState::RUNNING;
    }
    bool isFinished()
    {
        return this->state == QueryState::FINISHED? true: false;
    }

    bool isCanceled()
    {
        return this->state == QueryState::CANCELED? true: false;
    }

    bool isRunning()
    {
        return this->state == QueryState::RUNNING? true: false;
    }



};




#endif //OLVP_QUERYSTATEMACHINE_HPP
