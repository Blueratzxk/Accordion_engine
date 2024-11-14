//
// Created by zxk on 11/26/23.
//

#include "TaskServer.h"

shared_ptr<TaskManager> TaskServer::getTaskServer()
{
    return TaskServer::taskServer;
}
shared_ptr<CpuInfoCollector> TaskServer::getCpuInfoCollector()
{
    if(TaskServer::cpuInfoCollector->getTaskManager() == NULL)
        TaskServer::cpuInfoCollector->setTaskManager(TaskServer::getTaskServer());

    return TaskServer::cpuInfoCollector;
}

shared_ptr<TaskManager> TaskServer::taskServer = make_shared<TaskManager>();
shared_ptr<TaskResourceManager> TaskServer::taskResourceManager = make_shared<TaskResourceManager>(TaskServer::taskServer);
shared_ptr<CpuInfoCollector> TaskServer::cpuInfoCollector = make_shared<CpuInfoCollector>();