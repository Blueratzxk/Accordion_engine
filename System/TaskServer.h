//
// Created by zxk on 11/26/23.
//

#ifndef OLVP_TASKSERVER_H
#define OLVP_TASKSERVER_H


#include "../Execution/Task/TaskManager.hpp"
#include "../Execution/Task/TaskResource.hpp"
#include "../Execution/Task/Statistics/CPU/CpuInfoCollector.hpp"
class TaskServer
{
    static shared_ptr<TaskManager> taskServer;
    static shared_ptr<CpuInfoCollector> cpuInfoCollector;

public:
    static shared_ptr<TaskResourceManager>  taskResourceManager;

    static shared_ptr<TaskManager> getTaskServer();
    static shared_ptr<CpuInfoCollector> getCpuInfoCollector();

};


#endif //OLVP_TASKSERVER_H
