//
// Created by zxk on 7/6/24.
//

#ifndef OLVP_CPUINFOREADER_HPP
#define OLVP_CPUINFOREADER_HPP

#include "CpuInfo.hpp"

using namespace std;
class CpuInfoReader
{
    int coreNums = 0;
    pid_t processId;
    FILE *fp;
    char buf[128];
    string pPath;
public:
    CpuInfoReader()
    {
        this->coreNums = std::thread::hardware_concurrency();
        this->processId = getpid();
        pPath += "/proc/stat";

    }
    int getCoreNums()
    {
        return this->coreNums;
    }

    bool readCpuInfo(shared_ptr<CpuInfo> cpuinfo)
    {
        fp = fopen(pPath.c_str(),"r");

        if(fp == NULL) {
            fclose(fp);
            return false;
        }
        fgets(buf,sizeof(buf),fp);
        sscanf(buf,"%s%ld%ld%ld%ld%ld%ld%ld",cpuinfo->cpu,&cpuinfo->user,&cpuinfo->nice,&cpuinfo->sys,&cpuinfo->idle,&cpuinfo->iowait,&cpuinfo->irq,&cpuinfo->softirq);
        fclose(fp);

        return true;
    }



};

#endif //OLVP_CPUINFOREADER_HPP
