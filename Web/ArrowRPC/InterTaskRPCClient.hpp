//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKRPCCLIENT_HPP
#define OLVP_INTERTASKRPCCLIENT_HPP



#include "InterTaskArrowRPCClient.hpp"
#include "../../Split/RemoteSplit.hpp"
#include "../../Split/InterTaskSplit.hpp"

class InterTaskRPCClient : public std::enable_shared_from_this<InterTaskRPCClient>
{
    shared_ptr<InterTaskDataPageRPCBuffer> buffer;
    std::map<string,shared_ptr<ConnectorSplit>> taskIdToLocationMap;

    std::map<string,shared_ptr<InterTaskArrowRPCClient>> allClients;
    std::set<string> runningClients;
    std::set<shared_ptr<InterTaskArrowRPCClient>> completedClients;
    atomic<int> endPageNum = 0;
    mutex lock;

    atomic<bool> abortAll = false;


public:
    InterTaskRPCClient(){
        this->buffer = make_shared<InterTaskDataPageRPCBuffer>();

    }

    void abort()
    {
        this->abortAll = true;
    }
    bool isAbort()
    {
        return this->abortAll;
    }
    shared_ptr<InterTaskDataPageRPCBuffer> getBuffer()
    {
        return this->buffer;
    }


    void addLocation(shared_ptr<RemoteSplit> remoteSource)
    {
        lock.lock();
        shared_ptr<RemoteSplit> remote = remoteSource;
        string taskId = remoteSource->getTaskId()->ToString();
        if(this->taskIdToLocationMap.count(taskId) == 0) {
            this->taskIdToLocationMap[taskId] = remote;

            shared_ptr<InterTaskArrowRPCClient> client;
            string bufferIds;
            client = make_shared<InterTaskArrowRPCClient>(remote->getLocation()->getIp(),remote->getLocation()->getPort());
            bufferIds = remote->getLocation()->getBufferId();
            client->setBufferTarget(taskId,bufferIds,"0");



            this->allClients[taskId] = client;

        }
        lock.unlock();

    }


    void addInterTaskDataExchangePath(shared_ptr<InterTaskSplit> interTaskSource)
    {
        lock.lock();
        shared_ptr<InterTaskSplit> remote = interTaskSource;
        string taskId = interTaskSource->getTaskId()->ToString();
        if(this->taskIdToLocationMap.count(taskId) == 0) {
            this->taskIdToLocationMap[taskId] = remote;

            shared_ptr<InterTaskArrowRPCClient> client;
            string bufferId;
            client = make_shared<InterTaskArrowRPCClient>(remote->getLocation()->getIp(),remote->getLocation()->getPort());
            bufferId = remote->getLocation()->getBufferId();
            client->setInterTaskCacheTarget(taskId,remote->getSourceId(),bufferId);


            this->allClients[taskId] = client;

        }
        lock.unlock();

    }

    void broadcastNotes(string note)
    {
        for(auto client : allClients)
        {
            client.second->setNote(note);
        }
    }

    bool isFull()
    {
        bool fullTag = false;
        lock.lock();
        if(this->buffer->isFull())
            fullTag = true;
        else
            fullTag = false;
        lock.unlock();
        return fullTag;
    }

    void tuneBufferCapacity(string role)
    {
        this->buffer->tuneBufferCapacity(role);
    }

    shared_ptr<DataPage> pollPage()
    {
        return this->buffer->getPage();
    }

  ////  void scheduleAllClient()
 //   {
 //       for(auto client :this->allClients)
 //       {
  //          lock.lock();
  //          if(this->runningClients.count(client.first) == 0) {
  //              schedule(client.second);
  //              this->runningClients.insert(client.first);
  //          }
  //          lock.unlock();
  //      }
 //   }
    bool hasRunningClient()
    {
        if(this->runningClients.size() > 0)
            return true;
        else
            return false;
    }


    void scheduleAllClientOneRound(int pageNums)
    {



        lock.lock();

        if(this->abortAll) {
            if(this->runningClients.empty() && this->endPageNum < this->taskIdToLocationMap.size())
            {
                int compensation = this->taskIdToLocationMap.size() - this->endPageNum;
                for(int i = 0 ; i < compensation ; i++)
                    this->buffer->enqueuePages({DataPage::getEndPage()});
            }

            lock.unlock();
            return;
        }


        if(this->allClients.size() > 0)
            for(auto client :this->allClients)
            {
                if(this->runningClients.count(client.first) == 0) {
                    scheduleOneRound(client.first,client.second,pageNums);
                    this->runningClients.insert(client.first);
                }
            }
        lock.unlock();
    }
    void scheduleAllClientOneRoundByBufferCapacity()
    {
        lock.lock();

        if(this->abortAll) {
            if(this->runningClients.empty() && this->endPageNum < this->taskIdToLocationMap.size())
            {
                int compensation = this->taskIdToLocationMap.size() - this->endPageNum;
                for(int i = 0 ; i < compensation ; i++)
                    this->buffer->enqueuePages({DataPage::getEndPage()});
            }
            lock.unlock();
            return;
        }


        int vacant = this->buffer->getBufferVacant();
        int clientNums = this->allClients.size();

        if(clientNums == 0) {
            lock.unlock();
            return;
        }

        int avg = vacant/clientNums;
        if(avg > 0)
        {
            for(auto client :this->allClients)
            {
                if(this->runningClients.count(client.first) == 0) {
                    scheduleOneRound(client.first,client.second,avg);
                    this->runningClients.insert(client.first);
                }
            }
        }
        else
        {
            for(auto client :this->allClients)
            {
                if(this->runningClients.count(client.first) == 0) {
                    scheduleOneRound(client.first,client.second,1);
                    this->runningClients.insert(client.first);
                }
            }
        }

        lock.unlock();
    }
    bool clientOnceCompleted(string clientId,int tag,int &endPageCompensationNum)
    {
        bool flag = true;

        lock.lock();
        this->runningClients.erase(clientId);
        if(tag == 2) {
            flag = this->removeClient(clientId,endPageCompensationNum);
        }
        lock.unlock();
        return flag;
    }

    bool removeClient(string clientId,int &endPageCompensationNum)
    {
        if(this->allClients.count(clientId) > 0) {
            this->allClients.erase(clientId);
            this->endPageNum++;

            if(this->abortAll && this->runningClients.empty() && this->endPageNum < this->taskIdToLocationMap.size())
                endPageCompensationNum = this->taskIdToLocationMap.size() - this->endPageNum;
            else
                endPageCompensationNum = -1;


            return true;
        }
        else
            return false;

    }

    void scheduleOneRound(string clientId,shared_ptr<InterTaskArrowRPCClient> client,int pageNums)
    {

        try{
            thread th(startGetPageOneRound, clientId,shared_from_this(),buffer,client,pageNums);
            pthread_setname_np(th.native_handle(), (clientId+"_HHH").c_str());
            th.detach();
        }
        catch (exception&e)
        {
            cout << e.what() << endl;
            cout << "!"<<endl;
        }
    }

 //   void schedule(shared_ptr<ArrowRPCClient> client)
  //  {
 //       thread(startGetAllPage,buffer,client).detach();
  //  }
//    static void startGetAllPage(shared_ptr<InterTaskDataPageRPCBuffer> buffer,shared_ptr<ArrowRPCClient> client)
//    {
//        arrow::Status status = client->getAllBatchesWithCircle(*buffer);
//    }

    static void startGetPageOneRound(string clientId,shared_ptr<InterTaskRPCClient> RPC,shared_ptr<InterTaskDataPageRPCBuffer> buffer,shared_ptr<InterTaskArrowRPCClient> client,int dataSize)
    {
        int pageTypeTag = 0;
        int endPageCompensationNum = -1;
        vector<shared_ptr<DataPage>> pagesReturn;
        //  arrow::Status status = client->getOnceBatches(*buffer,dataSize,&pageTypeTag);
        arrow::Status status = client->getOnceBatches(pagesReturn,dataSize,&pageTypeTag);
        bool haveNotSendEndpage = RPC->clientOnceCompleted(clientId,pageTypeTag,endPageCompensationNum);

        if(RPC->isAbort() && haveNotSendEndpage)
        {
            if(pageTypeTag != 2) {
                pagesReturn.push_back(DataPage::getEndPage());
            }
        }

        if(haveNotSendEndpage)
            buffer->enqueuePages(pagesReturn);

        if(endPageCompensationNum > 0)
            for(int i = 0 ; i < endPageCompensationNum ; i++)
                buffer->enqueuePages({DataPage::getEndPage()});




    }



};


#endif //OLVP_INTERTASKRPCCLIENT_HPP
