//
// Created by zxk on 9/26/25.
//

#ifndef OLVP_PARAMETERIZEDEVENT_HPP
#define OLVP_PARAMETERIZEDEVENT_HPP

#include <mutex>
#include <condition_variable>
#include <list>
#include "Event.h"
class ParameterizedEvent: public Event
{
    std::mutex mtx;
    std::condition_variable event;
    bool ready = false;


    mutex lock;

    std::list<std::pair<std::string,std::string>> events;
public:
    ParameterizedEvent()
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

    void notify(std::string source,std::string info) override
    {
        std::unique_lock<std::mutex> lck(mtx);

        lock.lock();
        events.push_back({source,info});
        lock.unlock();

        this->ready = true;
        event.notify_all();
    }

    list<pair<std::string,std::string>> getEvents(){

        list<pair<std::string,std::string>> results;
        lock.lock();
        for(int i = 0 ; i < this->events.size() ; i++) {
            results.push_back(this->events.front());
            this->events.pop_front();
        }
        lock.unlock();

        return results;
    }

};

#endif //OLVP_PARAMETERIZEDEVENT_HPP
