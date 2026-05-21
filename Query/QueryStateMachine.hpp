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
    std::atomic<QueryState> state{};

    shared_ptr<Event> eventListener;
public:

    QueryState getState()
    {
        return this->state;
    }

    QueryStateMachine() {
        this->eventListener = make_shared<SimpleEvent>();
    }

    void finished()
    {
        this->state = QueryState::FINISHED;
        this->eventListener->notify();

    }

    void listen() {
        this->eventListener->listen();
    }

    void canceled()
    {
        this->state = QueryState::CANCELED;
        this->eventListener->notify();
    }
    void openIQRS()
    {
        this->state = QueryState::CANIQRS;
        this->eventListener->notify();
    }
    void planned()
    {
        this->state = QueryState::PLANNED;
        this->eventListener->notify();
    }
    void start()
    {
        this->state = QueryState::RUNNING;
        this->eventListener->notify();
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
