//
// Created by zxk on 6/7/23.
//

#ifndef OLVP_TASKRESULTFETCHER_HPP
#define OLVP_TASKRESULTFETCHER_HPP


//#include "../../../Web/Restful/Client.hpp"
#include "../Id/TaskId.hpp"
//#include "../TaskInfo.hpp"
#include <shared_mutex>
#include <thread>


//#include "../Id/TaskId.hpp"
///#include <shared_mutex>
#include <thread>
#include "../../../Web/ArrowRPC/RPCClient.hpp"
#include "../../../Split/WebLocation.hpp"


class TaskResultFetcher : public enable_shared_from_this<TaskResultFetcher>
{
    shared_ptr<TaskId> taskId;
    string targetTaskLocation;
    shared_ptr<RPCClient> client;


    string ip;
    string port;
    string bufferId;

    vector<shared_ptr<DataPage>> results;

    bool finished;

public:
    TaskResultFetcher(shared_ptr<TaskId> taskId,string ip,string port,string bufferId)
    {
        this->taskId = taskId;
        this->ip = ip;
        this->port = port;
        this->bufferId = bufferId;
        this->client = make_shared<RPCClient>();
        shared_ptr<RemoteSplit> remoteSplit = make_shared<RemoteSplit>(this->taskId, make_shared<Location>(this->ip,this->port,this->bufferId));
        this->client->addLocation(remoteSplit);
    }
    void schedule()
    {
        this->client->scheduleAllClientOneRound(10);
    }

    shared_ptr<DataPage> pollPage()
    {
        return this->client->pollPage();
    }


};

#endif //OLVP_TASKRESULTFETCHER_HPP
