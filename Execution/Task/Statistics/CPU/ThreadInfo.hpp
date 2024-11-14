//
// Created by zxk on 7/6/24.
//

#ifndef OLVP_THREADINFO_HPP
#define OLVP_THREADINFO_HPP
#include <iostream>
#include <set>
#include <thread>
#include <sys/types.h>
#include <unistd.h>
#include "spdlog/spdlog.h"
using namespace std;
class ThreadInfo
{
public:
    pid_t pid;
    long int utime,stime,cutime,cstime;

    ThreadInfo()
    {

    }

    long int computeThreadTime(ThreadInfo threadInfo2)
    {
        return (threadInfo2.utime+threadInfo2.stime) - (this->utime+this->stime);
    }

    long int computeProcessTime(ThreadInfo threadInfo2)
    {
        return (threadInfo2.utime+threadInfo2.stime+threadInfo2.cutime+threadInfo2.cstime) - (this->utime+this->stime+this->cutime+this->cstime);
    }


    void print() {
        spdlog::info(to_string(this->pid) + " " + to_string(this->utime)
                     + " " + to_string(this->stime) + " " + to_string(this->cutime)
                     + " " + to_string(this->cstime));
    }

};


#endif //OLVP_THREADINFO_HPP
