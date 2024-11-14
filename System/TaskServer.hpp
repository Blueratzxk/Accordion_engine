//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_TASKSERVER_HPP
#define OLVP_TASKSERVER_HPP


#include "../Execution/Task/TaskManager.hpp"
#include "../Execution/Task/TaskResource.hpp"

class TaskServer
{
    static shared_ptr<TaskManager> taskServer;
public:
    static shared_ptr<TaskResourceManager>  taskResourceManager;

    static shared_ptr<TaskManager> getTaskServer()
    {
        return taskServer;
    }

};

shared_ptr<TaskManager> TaskServer::taskServer = make_shared<TaskManager>();
shared_ptr<TaskResourceManager> TaskServer::taskResourceManager = make_shared<TaskResourceManager>(TaskServer::taskServer);




#endif //OLVP_TASKSERVER_HPP
