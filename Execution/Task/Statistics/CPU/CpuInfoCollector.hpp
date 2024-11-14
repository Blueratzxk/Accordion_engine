//
// Created by zxk on 7/3/24.
//

#ifndef OLVP_CPUINFOCOLLECTOR_HPP
#define OLVP_CPUINFOCOLLECTOR_HPP

#include "CpuInfoReader.hpp"
#include "ThreadInfoReader.hpp"
#include "TaskCpuUsageDescriptor.hpp"
//#include "../Execution/Task/TaskManager.hpp"
class threadUsage
{
public:
    string type;
    float usageValue;
};

class TasksCpuInfos
{
    shared_ptr<map<string,set<shared_ptr<threadUsage>>>> taskIdToCpuUsageDisplay = NULL;


    float nodeCpuUsage = 0;
    float processCpuUsage = 0;

    mutex lock;

public:

    TasksCpuInfos(){}



    void storeAllResult(map<string, float> tid_usages, map<string,map<string,set<pid_t >>> task_type_tids,float nodeCpuUsage,float processCpuUsage)
    {

        lock.lock();
        shared_ptr<map<string,set<shared_ptr<threadUsage>>>> display = make_shared<map<string,set<shared_ptr<threadUsage>>>>();

        map<pid_t,string> tidToType;

        map<string,set<pid_t>> task_tids;

        for(auto type : task_type_tids)
        {
            for(auto id: type.second)
            {
                if(task_tids.count(type.first) == 0) {
                    task_tids[type.first] = id.second;
                    for(auto dd : id.second)
                        tidToType[dd] = id.first;
                }
                else {
                    for(auto tid : id.second) {
                        task_tids[type.first].insert(tid);
                        tidToType[tid] = id.first;
                    }
                }
            }
        }


        for(auto task : task_tids)
        {
            for(auto tid : task.second)
            {
                if((*display).count(task.first) == 0) {

                    shared_ptr<threadUsage> tu = make_shared<threadUsage>();
                    tu->type = tidToType[tid];
                    tu->usageValue = tid_usages[to_string(tid)];
                    (*display)[task.first] = {tu};
                }
                else {
                    shared_ptr<threadUsage> tu = make_shared<threadUsage>();
                    tu->type = tidToType[tid];
                    tu->usageValue = tid_usages[to_string(tid)];
                    (*display)[task.first].insert(tu);
                }
            }
        }
        this->taskIdToCpuUsageDisplay = display;

        this->nodeCpuUsage = nodeCpuUsage;
        this->processCpuUsage = processCpuUsage;

        lock.unlock();
    }

    void storeNodeAndProcessUsage(float nodeCpuUsage,float processCpuUsage)
    {
        lock.lock();
        this->nodeCpuUsage = nodeCpuUsage;
        this->processCpuUsage = processCpuUsage;
        this->taskIdToCpuUsageDisplay = NULL;
        lock.unlock();
    }

    float getNodeCpuUsage()
    {
        return this->nodeCpuUsage;
    }
    float getProcessCpuUsage()
    {
        return this->processCpuUsage;
    }

    TaskCpuUsageDescriptor getCpuUsageByTaskId(string taskId)
    {
        TaskCpuUsageDescriptor descriptor;

        lock.lock();

        if(this->taskIdToCpuUsageDisplay != NULL)
        {
            auto re = (*this->taskIdToCpuUsageDisplay)[taskId];
            map<string,float> typeUsages;
            map<string,int> typeNums;

            string out;
            for (auto tid: re) {
                if(typeUsages.count(tid->type) == 0) {
                    typeUsages[tid->type] = tid->usageValue;
                    typeNums[tid->type] = 1;
                }
                else {
                    typeUsages[tid->type] += tid->usageValue;
                    typeNums[tid->type]++;
                }
            }

            for(auto type : typeUsages) {
                descriptor.addInfo(type.first, type.second);
            }
            for(auto type : typeNums) {
                descriptor.addTypeNums(type.first, type.second);
            }
        }

        lock.unlock();

        return descriptor;

    }

    void display()
    {
        spdlog::info("------------------------------------------------------");
         spdlog::info("Node CPU Usage:" + to_string(nodeCpuUsage));
         spdlog::info("Process CPU Usage:" + to_string(processCpuUsage));

        lock.lock();
         if(this->taskIdToCpuUsageDisplay != NULL) {
             for (auto usage: *(this->taskIdToCpuUsageDisplay)) {
                 string taskId = usage.first;
                 string tids;
                 float allusage = 0;
                 for (auto tid: usage.second) {
                     tids += (" " + to_string(tid->usageValue));
                     allusage += tid->usageValue;
                 }

                 spdlog::info("Task ID:" + usage.first + " Threads usages:" + to_string(allusage));
             }
         }
        lock.unlock();
        spdlog::info("------------------------------------------------------");
    }

    void displays()
    {
        spdlog::info("------------------------------------------------------");
        spdlog::info("Node CPU Usage:" + to_string(nodeCpuUsage));
        spdlog::info("Process CPU Usage:" + to_string(processCpuUsage));




        if(this->taskIdToCpuUsageDisplay != NULL) {
            for (auto usage: *(this->taskIdToCpuUsageDisplay)) {
                string taskId = usage.first;

                map<string,float> typeUsages;
                string out;
                for (auto tid: usage.second) {

                    if(typeUsages.count(tid->type) == 0)
                        typeUsages[tid->type] = tid->usageValue;
                    else
                        typeUsages[tid->type] += tid->usageValue;
                }

                for(auto type : typeUsages) {
                    out.append(type.first);
                    out.append(":");
                    out.append(to_string(type.second));
                    out.append(" ");
                }
                spdlog::info("Task ID:" + usage.first + " Threads usages:" + out);
            }
        }

        spdlog::info("------------------------------------------------------");
    }



};


class CpuInfoCollector
{
    set<pid_t> tids;
    map<string,map<string,set<pid_t>>> task_threads;
    shared_ptr<CpuInfoReader> cpuInfoReader;
    shared_ptr<ThreadInfoReader> threadInfoReader;

    list<shared_ptr<CpuInfo>> cpuInfosPre;
    list<shared_ptr<CpuInfo>> cpuInfosAfter;

    list<shared_ptr<ThreadInfo>> threadInfosPre;
    list<shared_ptr<ThreadInfo>> threadInfosAfter;

    shared_ptr<TasksCpuInfos> tasksCpuInfos;

    shared_ptr<TaskManager> taskManager = NULL;
    int sampleGap = 100000;

    atomic<bool> refresher = true;
    atomic<bool> refreshMode = false;

public:
    CpuInfoCollector()
    {
        this->cpuInfoReader = make_shared<CpuInfoReader>();
        this->threadInfoReader = make_shared<ThreadInfoReader>();
        this->tasksCpuInfos = make_shared<TasksCpuInfos>();
        releaseRefreshThread();
    }
    void setTaskManager(shared_ptr<TaskManager> taskManager)
    {
        this->taskManager = taskManager;
    }
    shared_ptr<TaskManager>  getTaskManager()
    {
        return this->taskManager = taskManager;
    }

    shared_ptr<TasksCpuInfos> getTasksCpuInfos()
    {
        return this->tasksCpuInfos;
    }

    bool getRefresher()
    {
        return this->refresher;
    }
    bool getRefreshMode()
    {
        return this->refreshMode;
    }

    void releaseRefreshThread()
    {
        this->refresher = true;
        thread(refresherThread,this).detach();
    }
    void closeRefresh()
    {
        this->refreshMode = false;
    }
    void openRefresh()
    {
        this->refreshMode = true;
    }

    static void refresherThread(CpuInfoCollector *cpuInfoCollector)
    {
        while(cpuInfoCollector->refresher)
        {
            if(cpuInfoCollector->getTaskManager() != NULL) {
                auto allRunningTaskContexts = cpuInfoCollector->getTaskManager()->getAllRunningTaskContexts();


                for (auto q: allRunningTaskContexts)
                    for (auto tc: q.second) {
                        cpuInfoCollector->gatherTids(tc->getTaskId()->ToString(), tc->getAllTaskTids());

                    }
                cpuInfoCollector->recordAllUsages();
            }
            if(cpuInfoCollector->refreshMode)
                cpuInfoCollector->tasksCpuInfos->displays();

            usleep(300000);

        }

    }


    float getNodeCpuUsage()
    {
        return this->tasksCpuInfos->getNodeCpuUsage();
    }
    float getCpuCoreNums()
    {
        return this->cpuInfoReader->getCoreNums();
    }

    float getProcessCpuUsage()
    {
        return this->tasksCpuInfos->getProcessCpuUsage();
    }
    void gatherTids(string taskId,map<string,set<pid_t>> ids)
    {

        this->task_threads[taskId] = ids;


        for(auto type : ids)
        {
            for(auto id : type.second)
                this->tids.insert(id);
        }
    }

    float computeThreadUsage(shared_ptr<CpuInfo> cpuInfoPre,shared_ptr<CpuInfo> cpuInfoAfter,
                            shared_ptr<ThreadInfo> threadInfoPre,shared_ptr<ThreadInfo> threadInfoAfter)
    {
        long int totalCpuTime = cpuInfoPre->computeTotalCpuTime(*cpuInfoAfter);
        long int totalThreadTime = threadInfoPre->computeThreadTime(*threadInfoAfter);
        float threadCPUUsage = ((float)totalThreadTime / (float)totalCpuTime * 100.000) * (float)this->cpuInfoReader->getCoreNums();
        return threadCPUUsage;
    }
    map<string,float> computeUsages() {



        map<string, float> usages;

        if(this->tids.empty())
            return usages;

        for (auto id: tids) {
            shared_ptr<CpuInfo> cpuInfo = make_shared<CpuInfo>();
            if (cpuInfoReader->readCpuInfo(cpuInfo))
                cpuInfosPre.push_back(cpuInfo);
            else
                cpuInfosPre.push_back(nullptr);

            shared_ptr<ThreadInfo> threadInfo = make_shared<ThreadInfo>();
            if(threadInfoReader->readThreadInfoByTid(id,threadInfo))
                threadInfosPre.push_back(threadInfo);
            else
                threadInfosPre.push_back(nullptr);
        }

        usleep(this->sampleGap);

        for (auto id: tids) {
            shared_ptr<CpuInfo> cpuInfo = make_shared<CpuInfo>();
            if (cpuInfoReader->readCpuInfo(cpuInfo))
                cpuInfosAfter.push_back(cpuInfo);
            else
                cpuInfosAfter.push_back(nullptr);

            shared_ptr<ThreadInfo> threadInfo = make_shared<ThreadInfo>();
            if(threadInfoReader->readThreadInfoByTid(id,threadInfo))
                threadInfosAfter.push_back(threadInfo);
            else
                threadInfosAfter.push_back(nullptr);
        }


        auto cpuPre = cpuInfosPre.begin();
        auto cpuAfter = cpuInfosAfter.begin();
        auto threadPre = threadInfosPre.begin();
        auto threadAfter = threadInfosAfter.begin();


        for (int i = 0; i < cpuInfosPre.size(); ++i) {
            if(*(cpuPre) != nullptr && *(cpuAfter) != nullptr && *(threadPre) != nullptr && *(threadAfter) != nullptr ) {

                float re = this->computeThreadUsage(*cpuPre,*cpuAfter,*threadPre,*threadAfter);
                usages[to_string((*threadPre)->pid)] = re;

            }
            ++cpuPre;
            ++cpuAfter;
            ++threadPre;
            ++threadAfter;
        }

        float cpuuse = cpuInfosPre.back()->computeTotalCpuUsage(*cpuInfosAfter.back()) * (float)this->cpuInfoReader->getCoreNums();
        spdlog::info("------------------------------------------------------");
        spdlog::info("Node CPU Usage:" + to_string(cpuuse));
        for(auto usage : usages)
        {
            spdlog::info("Thread ID:"+usage.first+" Usage:"+ to_string(usage.second));
        }
        spdlog::info("------------------------------------------------------");

        cpuInfosPre.clear();
        cpuInfosAfter.clear();
        threadInfosPre.clear();
        threadInfosAfter.clear();
        task_threads.clear();
        tids.clear();

        return usages;

    }

    void showCurrentUsages()
    {
        this->tasksCpuInfos->display();
    }

    void recordAllUsages() {

        map<string, float> usages;

        if(this->tids.empty()) {
            float nodecpu,processcpu;
            this->recordNodeAndProcessCpuUsage(&nodecpu,&processcpu);
            this->tasksCpuInfos->storeNodeAndProcessUsage(nodecpu,processcpu);
            return;
        }


        shared_ptr<ThreadInfo> processInfoPre = make_shared<ThreadInfo>();
        shared_ptr<ThreadInfo> processInfoAfter = make_shared<ThreadInfo>();



        for (auto id: tids) {
            shared_ptr<CpuInfo> cpuInfo = make_shared<CpuInfo>();
            if (cpuInfoReader->readCpuInfo(cpuInfo))
                cpuInfosPre.push_back(cpuInfo);
            else
                cpuInfosPre.push_back(nullptr);

            shared_ptr<ThreadInfo> threadInfo = make_shared<ThreadInfo>();
            if(threadInfoReader->readThreadInfoByTid(id,threadInfo))
                threadInfosPre.push_back(threadInfo);
            else
                threadInfosPre.push_back(nullptr);
        }
        this->threadInfoReader->readProcessInfo(processInfoPre);
        usleep(this->sampleGap);


        for (auto id: tids) {
            shared_ptr<CpuInfo> cpuInfo = make_shared<CpuInfo>();
            if (cpuInfoReader->readCpuInfo(cpuInfo))
                cpuInfosAfter.push_back(cpuInfo);
            else
                cpuInfosAfter.push_back(nullptr);

            shared_ptr<ThreadInfo> threadInfo = make_shared<ThreadInfo>();
            if(threadInfoReader->readThreadInfoByTid(id,threadInfo))
                threadInfosAfter.push_back(threadInfo);
            else
                threadInfosAfter.push_back(nullptr);
        }
        this->threadInfoReader->readProcessInfo(processInfoAfter);

        auto cpuPre = cpuInfosPre.begin();
        auto cpuAfter = cpuInfosAfter.begin();
        auto threadPre = threadInfosPre.begin();
        auto threadAfter = threadInfosAfter.begin();


        for (int i = 0; i < cpuInfosPre.size(); ++i) {
            if(*(cpuPre) != nullptr && *(cpuAfter) != nullptr && *(threadPre) != nullptr && *(threadAfter) != nullptr ) {

                float re = this->computeThreadUsage(*cpuPre,*cpuAfter,*threadPre,*threadAfter);
                usages[to_string((*threadPre)->pid)] = re;

            }
            ++cpuPre;
            ++cpuAfter;
            ++threadPre;
            ++threadAfter;
        }

        float cpuuse = cpuInfosPre.back()->computeTotalCpuUsage(*cpuInfosAfter.back()) * (float)this->cpuInfoReader->getCoreNums();

        float processcpuuse = this->computeThreadUsage(cpuInfosPre.back(),cpuInfosAfter.back(),processInfoPre,processInfoAfter);

        this->tasksCpuInfos->storeAllResult(usages,this->task_threads,cpuuse,processcpuuse);


        //spdlog::info("------------------------------------------------------");
       // spdlog::info("Node CPU Usage:" + to_string(cpuuse));
       // spdlog::info("Process CPU Usage:" + to_string(processcpuuse));
       // for(auto usage : usages)
       // {
       //     spdlog::info("Thread ID:"+usage.first+" Usage:"+ to_string(usage.second));
       // }
       // spdlog::info("------------------------------------------------------");
        cpuInfosPre.clear();
        cpuInfosAfter.clear();
        threadInfosPre.clear();
        threadInfosAfter.clear();
        task_threads.clear();
        tids.clear();

    }


    map<string,float> computeAllUsages() {



        map<string, float> usages;

        if(this->tids.empty()) {
            this->computeNodeAndProcessCpuUsage();
            return usages;
        }


        shared_ptr<ThreadInfo> processInfoPre = make_shared<ThreadInfo>();
        shared_ptr<ThreadInfo> processInfoAfter = make_shared<ThreadInfo>();



        for (auto id: tids) {
            shared_ptr<CpuInfo> cpuInfo = make_shared<CpuInfo>();
            if (cpuInfoReader->readCpuInfo(cpuInfo))
                cpuInfosPre.push_back(cpuInfo);
            else
                cpuInfosPre.push_back(NULL);

            shared_ptr<ThreadInfo> threadInfo = make_shared<ThreadInfo>();
            if(threadInfoReader->readThreadInfoByTid(id,threadInfo))
                threadInfosPre.push_back(threadInfo);
            else
                threadInfosPre.push_back(NULL);
        }
        this->threadInfoReader->readProcessInfo(processInfoPre);
        usleep(this->sampleGap);


        for (auto id: tids) {
            shared_ptr<CpuInfo> cpuInfo = make_shared<CpuInfo>();
            if (cpuInfoReader->readCpuInfo(cpuInfo))
                cpuInfosAfter.push_back(cpuInfo);
            else
                cpuInfosAfter.push_back(NULL);

            shared_ptr<ThreadInfo> threadInfo = make_shared<ThreadInfo>();
            if(threadInfoReader->readThreadInfoByTid(id,threadInfo))
                threadInfosAfter.push_back(threadInfo);
            else
                threadInfosAfter.push_back(NULL);
        }
        this->threadInfoReader->readProcessInfo(processInfoAfter);

        auto cpuPre = cpuInfosPre.begin();
        auto cpuAfter = cpuInfosAfter.begin();
        auto threadPre = threadInfosPre.begin();
        auto threadAfter = threadInfosAfter.begin();


        for (int i = 0; i < cpuInfosPre.size(); ++i) {
            if(*(cpuPre) != NULL && *(cpuAfter) != NULL && *(threadPre) != NULL && *(threadAfter) != NULL ) {

                float re = this->computeThreadUsage(*cpuPre,*cpuAfter,*threadPre,*threadAfter);
                usages[to_string((*threadPre)->pid)] = re;

            }
            ++cpuPre;
            ++cpuAfter;
            ++threadPre;
            ++threadAfter;
        }

        float cpuuse = cpuInfosPre.back()->computeTotalCpuUsage(*cpuInfosAfter.back()) * (float)this->cpuInfoReader->getCoreNums();

        float processcpuuse = this->computeThreadUsage(cpuInfosPre.back(),cpuInfosAfter.back(),processInfoPre,processInfoAfter);

        spdlog::info("------------------------------------------------------");
        spdlog::info("Node CPU Usage:" + to_string(cpuuse));
        spdlog::info("Process CPU Usage:" + to_string(processcpuuse));
        for(auto usage : usages)
        {
            spdlog::info("Thread ID:"+usage.first+" Usage:"+ to_string(usage.second));
        }
        spdlog::info("------------------------------------------------------");
        cpuInfosPre.clear();
        cpuInfosAfter.clear();
        threadInfosPre.clear();
        threadInfosAfter.clear();
        task_threads.clear();
        tids.clear();

        return usages;

    }
    void recordNodeAndProcessCpuUsage(float *nodecpu, float *processcpu) {
        shared_ptr<CpuInfo> cpuInfoPre = make_shared<CpuInfo>();
        shared_ptr<CpuInfo> cpuInfoAfter = make_shared<CpuInfo>();


        shared_ptr<ThreadInfo> processInfoPre = make_shared<ThreadInfo>();
        shared_ptr<ThreadInfo> processInfoAfter = make_shared<ThreadInfo>();

        this->cpuInfoReader->readCpuInfo(cpuInfoPre);
        this->threadInfoReader->readProcessInfo(processInfoPre);
        usleep(this->sampleGap);
        this->cpuInfoReader->readCpuInfo(cpuInfoAfter);
        this->threadInfoReader->readProcessInfo(processInfoAfter);

        float cpuuse = cpuInfoPre->computeTotalCpuUsage(*cpuInfoAfter) * this->cpuInfoReader->getCoreNums();

        float processcpuuse = this->computeThreadUsage(cpuInfoPre,cpuInfoAfter,processInfoPre,processInfoAfter);

        (*nodecpu) = cpuuse;
        (*processcpu) = processcpuuse;

     //   spdlog::info("------------------------------------------------------");
    //    spdlog::info("Node CPU Usage:" + to_string(cpuuse));
    //    spdlog::info("Process CPU Usage:" + to_string(processcpuuse));
    //    spdlog::info("------------------------------------------------------");
     //   return cpuuse;
    }
    float computeNodeAndProcessCpuUsage() {
        shared_ptr<CpuInfo> cpuInfoPre = make_shared<CpuInfo>();
        shared_ptr<CpuInfo> cpuInfoAfter = make_shared<CpuInfo>();


        shared_ptr<ThreadInfo> processInfoPre = make_shared<ThreadInfo>();
        shared_ptr<ThreadInfo> processInfoAfter = make_shared<ThreadInfo>();

        this->cpuInfoReader->readCpuInfo(cpuInfoPre);
        this->threadInfoReader->readProcessInfo(processInfoPre);
        usleep(this->sampleGap);
        this->cpuInfoReader->readCpuInfo(cpuInfoAfter);
        this->threadInfoReader->readProcessInfo(processInfoAfter);

        float cpuuse = cpuInfoPre->computeTotalCpuUsage(*cpuInfoAfter) * this->cpuInfoReader->getCoreNums();

        float processcpuuse = this->computeThreadUsage(cpuInfoPre,cpuInfoAfter,processInfoPre,processInfoAfter);


           spdlog::info("------------------------------------------------------");
            spdlog::info("Node CPU Usage:" + to_string(cpuuse));
           spdlog::info("Process CPU Usage:" + to_string(processcpuuse));
            spdlog::info("------------------------------------------------------");
           return cpuuse;
    }
    float computeNodeCpuUsage() {

        shared_ptr<CpuInfo> cpuInfoPre = make_shared<CpuInfo>();
        shared_ptr<CpuInfo> cpuInfoAfter = make_shared<CpuInfo>();


        this->cpuInfoReader->readCpuInfo(cpuInfoPre);
        usleep(this->sampleGap);
        this->cpuInfoReader->readCpuInfo(cpuInfoAfter);

        float cpuuse = cpuInfoPre->computeTotalCpuUsage(*cpuInfoAfter) * this->cpuInfoReader->getCoreNums();
        spdlog::info("------------------------------------------------------");
        spdlog::info("Node CPU Usage:" + to_string(cpuuse));
        spdlog::info("------------------------------------------------------");
        return cpuuse;

    }
    float computeProcessUsage() {

        shared_ptr<CpuInfo> cpuInfoPre = make_shared<CpuInfo>();
        shared_ptr<CpuInfo> cpuInfoAfter = make_shared<CpuInfo>();


        shared_ptr<ThreadInfo> processInfoPre = make_shared<ThreadInfo>();
        shared_ptr<ThreadInfo> processInfoAfter = make_shared<ThreadInfo>();

        this->cpuInfoReader->readCpuInfo(cpuInfoPre);
        this->threadInfoReader->readProcessInfo(processInfoPre);
        usleep(this->sampleGap);
        this->cpuInfoReader->readCpuInfo(cpuInfoAfter);
        this->threadInfoReader->readProcessInfo(processInfoAfter);

        float cpuuse = this->computeThreadUsage(cpuInfoPre,cpuInfoAfter,processInfoPre,processInfoAfter);


        spdlog::info("------------------------------------------------------");
        spdlog::info("Process CPU Usage:" + to_string(cpuuse));
        spdlog::info("------------------------------------------------------");
        return cpuuse;
    }


};



#endif //OLVP_CPUINFOCOLLECTOR_HPP
