//
// Created by zxk on 7/6/24.
//

#ifndef OLVP_THREADINFOREADER_HPP
#define OLVP_THREADINFOREADER_HPP

#include "ThreadInfo.hpp"
class ThreadInfoReader
{
    pid_t processId;
    FILE *fp;
    char buf[128];
    string pPath;

public:
    ThreadInfoReader() {
        this->processId = getpid();
        pPath += "/proc/";
        pPath += to_string(processId);
        pPath += "/task/";
    }

    bool readThreadInfoByTid(pid_t  tid,shared_ptr<ThreadInfo> threadInfo)
    {
        string path = pPath + (to_string(tid)+"/stat");


        fp = fopen(path.c_str(),"r");

        if(fp == NULL) {
            return false;
        }
        fgets(buf,sizeof(buf),fp);

        sscanf(buf,
               "%d %*[^ ] "
               "%*c "                       // state
               "%*d %*d %*d %*d %*d "       // ppid, pgrp, session, tty_nr, tpgid
               "%*u "                        // flags (was %lu before Linux 2.6)
               "%*u %*u %*u %*u "           // minflt, cminflt, majflt, cmajflt
               "%lu %lu %ld %ld ",           // utime, stime, cutime, cstime
               &threadInfo->pid,&threadInfo->utime, &threadInfo->stime, &threadInfo->cutime, &threadInfo->cstime);


        fclose(fp);

        return true;
    }

    bool readProcessInfo(shared_ptr<ThreadInfo> threadInfo)
    {
        string path = "/proc/"+to_string(processId)+"/stat";


        fp = fopen(path.c_str(),"r");

        if(fp == NULL) {
            fclose(fp);
            return false;
        }
        fgets(buf,sizeof(buf),fp);

        sscanf(buf,
               "%d %*[^ ] "
               "%*c "                       // state
               "%*d %*d %*d %*d %*d "       // ppid, pgrp, session, tty_nr, tpgid
               "%*u "                        // flags (was %lu before Linux 2.6)
               "%*u %*u %*u %*u "           // minflt, cminflt, majflt, cmajflt
               "%lu %lu %ld %ld ",           // utime, stime, cutime, cstime
               &threadInfo->pid,&threadInfo->utime, &threadInfo->stime, &threadInfo->cutime, &threadInfo->cstime);

        fclose(fp);

        return true;
    }

};

#endif //OLVP_THREADINFOREADER_HPP
