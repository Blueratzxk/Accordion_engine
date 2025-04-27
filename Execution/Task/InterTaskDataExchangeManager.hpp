//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKDATAEXCHANGEMANAGER_HPP
#define OLVP_INTERTASKDATAEXCHANGEMANAGER_HPP

#include <memory>
#include <map>
#include <vector>
#include "../Buffer/InterTaskSimpleOutputBuffer.hpp"
#include "../../Web/ArrowRPC/InterTaskRPCClient.hpp"
using namespace std;

class InterTaskDataHandle
{
    bool status;
    string taskId;
    string componentId;
    string bufferId;
public:
    InterTaskDataHandle(string taskId,string componentId,string bufferId)
    {
        this->taskId = taskId;
        this->componentId = componentId;
        this->bufferId = bufferId;
        this->status = true;
    }
    InterTaskDataHandle(bool status,string taskId,string componentId,string bufferId)
    {
        this->status = status;
        this->taskId = taskId;
        this->componentId = componentId;
        this->bufferId = bufferId;
    }

    bool getStatus(){return this->status;}
    string getTaskId(){return this->taskId;}
    string getComponentId(){return this->componentId;}
    string getBufferId(){return this->bufferId;}


    static string Serialize(InterTaskDataHandle interTaskDataHandle)
    {
        nlohmann::json json;

        json["status"] = interTaskDataHandle.status;
        json["taskId"] = interTaskDataHandle.taskId;
        json["componentId"] = interTaskDataHandle.componentId;
        json["bufferId"] = interTaskDataHandle.bufferId;


        string result = json.dump();
        return result;
    }

    static shared_ptr<InterTaskDataHandle> Deserialize(string interTaskDataHandle)
    {
        nlohmann::json json = nlohmann::json::parse(interTaskDataHandle);


        auto result = make_shared<InterTaskDataHandle>(json["status"],json["taskId"],json["componentId"],json["bufferId"]);

        return  result;
    }


};

class InterTaskDataExchangeManager:public enable_shared_from_this<InterTaskDataExchangeManager>{

    map<string, std::shared_ptr<InterTaskRPCClient>> interTaskRpcClients;
    map<string, shared_ptr<InterTaskSimpleOutputBuffer>> pageCaches;
public:
    InterTaskDataExchangeManager() {
        savePages("test",{});
    }


    void savePages(string componentId, vector<shared_ptr<DataPage>> pages) {
        if (!pageCaches.contains(componentId)) {
            pageCaches[componentId] = make_shared<InterTaskSimpleOutputBuffer>();
            pageCaches[componentId]->enqueue(pages);
        } else
            pageCaches[componentId]->enqueue(pages);
    }

    vector<shared_ptr<DataPage>> takePages(string componentId, string bufferId, int pageNums) {

        if (!pageCaches.contains(componentId))
            return {};

        auto re = pageCaches[componentId]->getPages(bufferId, 0, pageNums);
        for(auto rePage : re)
            if(rePage->isEndPage())
                pageCaches.erase(componentId);

        return re;
    }

    vector<shared_ptr<DataPage>> requestRemoteInterTaskPages(string taskId,string componentId, string bufferId,string ip,string port) {

        TaskId id;
        interTaskRpcClients[componentId] = make_shared<InterTaskRPCClient>();
        interTaskRpcClients[componentId]->addInterTaskDataExchangePath(make_shared<InterTaskSplit>(id.StringToObject(taskId),componentId,
                                                                                                   make_shared<Location>(ip,"9081",bufferId)));

        vector<shared_ptr<DataPage>> allPages;
        shared_ptr<DataPage> page = NULL;

        do {
            interTaskRpcClients[componentId]->scheduleAllClientOneRound(1000);
            page = interTaskRpcClients[componentId]->pollPage();
            if(page != NULL)
                allPages.push_back(page);
        }
        while(page==NULL || !page->isEndPage());

        interTaskRpcClients.erase(componentId);

        return allPages;
    }

};
#endif //OLVP_INTERTASKDATAEXCHANGEMANAGER_HPP
