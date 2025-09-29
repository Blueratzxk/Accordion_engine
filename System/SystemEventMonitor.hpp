//
// Created by zxk on 9/26/25.
//

#ifndef OLVP_SYSTEMEVENTMONITOR_HPP
#define OLVP_SYSTEMEVENTMONITOR_HPP
#include <list>
#include "../Execution/Event/ParameterizedEvent.hpp"
class SystemEventMonitor
{

    shared_ptr<ParameterizedEvent> parameterizedEvent;

public:
    SystemEventMonitor()
    {
        parameterizedEvent = make_shared<ParameterizedEvent>();
    }

    shared_ptr<ParameterizedEvent> getParameterizedEvent()
    {
        return this->parameterizedEvent;
    }

    void startMonitor()
    {
        thread monitor(eventMonitor,this);
        monitor.detach();
    }

    static void eventMonitor(SystemEventMonitor *systemEventMonitor){

        while(true)
        {
            systemEventMonitor->parameterizedEvent->listen();
            auto events = systemEventMonitor->parameterizedEvent->getEvents();
            for(auto event : events)
            {
                thread responser(eventResponser,event.first,event.second);
                responser.detach();
            }


        }

    }

    static void eventResponser(string eventSource,string eventInfo){

        string cleanIp = eventInfo;
        QueryServer::queryServer->drainNode(eventInfo);
    }

};


#endif //OLVP_SYSTEMEVENTMONITOR_HPP
