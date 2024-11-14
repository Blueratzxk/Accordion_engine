//
// Created by zxk on 7/6/24.
//

#ifndef OLVP_CPUINFO_HPP
#define OLVP_CPUINFO_HPP
#include <iostream>
#include <set>
#include <thread>
#include <sys/types.h>
#include <unistd.h>
#include "spdlog/spdlog.h"
using namespace std;
class CpuInfo
{
public:
    char cpu[5];
    long int user,nice,sys,idle,iowait,irq,softirq;


    CpuInfo()
    {

    }

    float computeTotalCpuUsage(CpuInfo cpuInfo2)
    {
        long int all1 = user+nice+sys+idle+iowait+irq+softirq;
        long int all2 = cpuInfo2.user+cpuInfo2.nice+cpuInfo2.sys+cpuInfo2.idle+cpuInfo2.iowait+cpuInfo2.irq+cpuInfo2.softirq;
        float usage = (float)(all2-all1-(cpuInfo2.idle-idle)) / (all2-all1)*100 ;
        return usage;
    }
    long int computeTotalCpuTime(CpuInfo cpuInfo2)
    {
        long int all1 = user+nice+sys+idle+iowait+irq+softirq;
        long int all2 = cpuInfo2.user+cpuInfo2.nice+cpuInfo2.sys+cpuInfo2.idle+cpuInfo2.iowait+cpuInfo2.irq+cpuInfo2.softirq;
        return all2-all1;
    }

    void print()
    {
        spdlog::info(string(cpu)+" "+to_string(this->user)
                     +" "+to_string(this->nice)+" "+to_string(this->sys)
                     +" "+to_string(this->idle)+" "+to_string(this->iowait)
                     +" "+to_string(this->irq)
                     +" "+to_string(this->softirq));
    }

};

#endif //OLVP_CPUINFO_HPP
