//
// Created by zxk on 7/5/24.
//

#ifndef OLVP_EVENT_H
#define OLVP_EVENT_H

class Event
{
public:
    virtual void listen() = 0;
    virtual void notify() = 0;
};

#endif //OLVP_EVENT_H
