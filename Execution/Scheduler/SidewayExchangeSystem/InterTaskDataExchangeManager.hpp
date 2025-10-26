//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKDATAEXCHANGEMANAGER_HPP
#define OLVP_INTERTASKDATAEXCHANGEMANAGER_HPP

#include <memory>
#include <map>
#include <vector>
#include "../../Buffer/InterTaskSimpleOutputBuffer.hpp"
#include "../../../Web/ArrowRPC/InterTaskRPCClient.hpp"
using namespace std;

class InterTaskDataExchangeManager:public enable_shared_from_this<InterTaskDataExchangeManager>{

    map<string, std::shared_ptr<InterTaskRPCClient>> interTaskRpcClients;
    map<string, shared_ptr<InterTaskSimpleOutputBuffer>> pageCaches;
public:
    InterTaskDataExchangeManager() {
        savePages("test",{});
    }


    void savePages(string componentId, vector<shared_ptr<DataPage>> pages) {

        spdlog::info("Save page" + componentId+"!");
        if (!pageCaches.contains(componentId)) {
            pageCaches[componentId] = make_shared<InterTaskSimpleOutputBuffer>();
            pageCaches[componentId]->enqueue(pages);
        } else
            pageCaches[componentId]->enqueue(pages);
    }

    vector<shared_ptr<DataPage>> takePages(string componentId, string bufferId, int pageNums) {

        spdlog::info("Take page" + componentId+"!");
        if (!pageCaches.contains(componentId))
            return {};

        auto re = pageCaches[componentId]->getPages(bufferId, 0, pageNums);

        spdlog::info("Take page" + componentId+" ok!");
        for(auto rePage : re) {


            if (rePage->isEndPage())
                pageCaches.erase(componentId);
        }

        return re;
    }

    vector<shared_ptr<DataPage>> requestRemoteInterTaskPages(string taskId,string ip,string port,string sourceId, string bufferId) {

        TaskId id;
        interTaskRpcClients[sourceId] = make_shared<InterTaskRPCClient>();
        interTaskRpcClients[sourceId]->addInterTaskDataExchangePath(make_shared<InterTaskSplit>(id.StringToObject(taskId),sourceId,
                                                                                                   make_shared<Location>(ip,"9081",bufferId)));

        vector<shared_ptr<DataPage>> allPages;
        shared_ptr<DataPage> page = NULL;

        do {
            interTaskRpcClients[sourceId]->scheduleAllClientOneRound(1000);
            page = interTaskRpcClients[sourceId]->pollPage();
            if(page != NULL)
                allPages.push_back(page);
        }
        while(page==NULL || !page->isEndPage());

        interTaskRpcClients.erase(sourceId);

        return allPages;
    }

};
#endif //OLVP_INTERTASKDATAEXCHANGEMANAGER_HPP
