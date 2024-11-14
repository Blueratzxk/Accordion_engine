//
// Created by zxk on 11/15/23.
//

#ifndef OLVP_TIMER_HPP
#define OLVP_TIMER_HPP

#include "TimeCommon.hpp"
class Timer
{
    long long start = 0;
    bool isSet = false;

public:
    Timer()
    {

    }

    void set()
    {
        if(this->isSet == true)
            return;
        else {
            this->start = TimeCommon::getCurrentTimeStamp();
            this->isSet = true;
        }
    }

    bool checkGap(long long gap)
    {
        long long now = TimeCommon::getCurrentTimeStamp();
        if(now - start >= gap) {
            this->isSet = false;
            this->start = 0;
            return true;
        }
        else
            return false;
    }

};


#endif //OLVP_TIMER_HPP
