//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_REMOTETASKFACTORY_HPP
#define OLVP_REMOTETASKFACTORY_HPP
#include "RemoteTask.hpp"
class RemoteTaskFactory
{

public:
    RemoteTaskFactory(){

    }

    shared_ptr<HttpRemoteTask> createRemoteTask(shared_ptr<Event> eventLister,shared_ptr<TaskId> taskId,shared_ptr<PlanFragment> fragment,string httpRequestLocation,
                                                shared_ptr<OutputBufferSchema> schema,shared_ptr<TaskSource> initial_taskSources,shared_ptr<Session> session)
    {
        return make_shared<HttpRemoteTask>(eventLister,taskId,fragment,httpRequestLocation,schema,initial_taskSources,session);
    }

};
#endif //OLVP_REMOTETASKFACTORY_HPP
