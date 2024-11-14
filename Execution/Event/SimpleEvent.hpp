//
// Created by zxk on 7/5/24.
//

#ifndef OLVP_SIMPLEEVENT_HPP
#define OLVP_SIMPLEEVENT_HPP

#include <mutex>
#include <condition_variable>
#include "Event.h"
class SimpleEvent: public Event
{
    std::mutex mtx;
    std::condition_variable event;
    bool ready;

public:
    SimpleEvent()
    {

    }

    void listen() override
    {
        std::unique_lock<std::mutex> lck(mtx);
        while (!ready) event.wait(lck);
        this->ready = false;

    }

    void notify() override
    {
        std::unique_lock<std::mutex> lck(mtx);
        this->ready = true;
        event.notify_all();
    }

};


#endif //OLVP_SIMPLEEVENT_HPP
