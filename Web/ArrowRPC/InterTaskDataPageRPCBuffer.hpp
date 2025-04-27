//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKDATAPAGERPCBUFFER_HPP
#define OLVP_INTERTASKDATAPAGERPCBUFFER_HPP


#include "../../Page/DataPage.hpp"
#include "../../common.h"
#include "tbb/concurrent_queue.h"

class InterTaskDataPageRPCBuffer
{
    tbb::concurrent_queue<shared_ptr<DataPage>> pageBuffer;
    atomic<int> bufferCapacity = 1;



    enum tuningRoles {None,consumer,producer};
    std::atomic<tuningRoles> curRole = None;
    int expandTime = 0;

    shared_ptr<std::chrono::system_clock::time_point> lastTurnUpTime = NULL;

    int maxExpand = bufferCapacity * 2;

public:
    InterTaskDataPageRPCBuffer(){

    }

    void enqueuePages(vector<shared_ptr<DataPage>> pages)
    {
        for(auto page : pages)
            pageBuffer.push(page);

        tuneBufferCapacity("producer");
    }


    shared_ptr<DataPage> getPage()
    {
        shared_ptr<DataPage> pageToGet = NULL;

        if(!pageBuffer.empty()) {


            if (!this->pageBuffer.try_pop(pageToGet)) {
                if (pageBuffer.empty()) {
                    tuneBufferCapacity("consumer");
                }
            }
        }
        else {

            tuneBufferCapacity("consumer");
        }


        return pageToGet;
    }

    void resetBufferCapacity(int cap)
    {
        if(this->lastTurnUpTime != NULL) {

            int newCap = cap;
            if(newCap < 1)
                newCap = 1;
            this->maxExpand = newCap *2;

            this->bufferCapacity = newCap;

        }


    }

    void tuneBufferCapacity(string role)
    {
        if(role == "producer" && this->curRole == producer)
            return;
        if(role == "consumer" && this->curRole == consumer)
            return;

        if(role == "producer" && this->isFull()) {
            this->curRole = producer;

        }

        if(role == "consumer" && this->isEmpty()) {
            this->curRole = consumer;
            this->expandBufferCapacity();
        }

        if(role == "producer")
            this->curRole = producer;
        if(role == "consumer")
            this->curRole = consumer;


    }

    void expandBufferCapacity()
    {
        this->bufferCapacity+=1;

        //       if(this->bufferCapacity > this->maxExpand)
        //          this->bufferCapacity = this->maxExpand;

        if(this->bufferCapacity >= INT_MAX)
            this->bufferCapacity = INT_MAX;

        this->lastTurnUpTime = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());


        //  spdlog::info("expand!"+to_string(bufferCapacity));
    }

    void reduceBufferCapacity()
    {
        this->bufferCapacity-=5;
        if(this->bufferCapacity <= 1)
            this->bufferCapacity = 1;
        spdlog::info("reduce!"+to_string(bufferCapacity));
    }

    int getBufferVacant()
    {
        return this->bufferCapacity - this->pageBuffer.unsafe_size();
    }
    bool isFull()
    {
        int size = this->pageBuffer.unsafe_size();
        if(size > this->bufferCapacity){

            return true;
        }
        else
            return false;
    }
    bool isEmpty()
    {
        int size = this->pageBuffer.unsafe_size();
        if(size <= 0){

            return true;
        }
        else
            return false;
    }


};



#endif //OLVP_INTERTASKDATAPAGERPCBUFFER_HPP
