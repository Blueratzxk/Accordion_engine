//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_BROADCASTOUTPUTBUFFER_HPP
#define OLVP_BROADCASTOUTPUTBUFFER_HPP


#include "OutputBuffer.hpp"
#include "ClientBuffer.hpp"
#include "OutputBufferSchema.hpp"
#include "tbb/concurrent_map.h"


class BroadcastOutputBuffer: public OutputBuffer
{

    int pageNumsLimit = 20;
    shared_ptr<OutputBufferSchema>bufferSchema = OutputBufferSchema::createInitialEmptyOutputBufferSchema(OutputBufferSchema::BufferType::BROADCAST);
    tbb::concurrent_map<string,shared_ptr<ClientBuffer>> buffers;

    atomic<int> maxBufferId = 0;
    bool endPageFound = false;

    vector<shared_ptr<DataPage>> allPages;


    mutex lock;

    shared_ptr<TaskContext> taskContext = NULL;

    atomic<int> outputOperatorCount = 0;
    atomic<int> endPageNum = 0;




public:
    BroadcastOutputBuffer(){
        this->setOutputBuffersSchema(*bufferSchema);
    }

    BroadcastOutputBuffer(OutputBufferSchema schema){
        this->setOutputBuffersSchema(schema);
    }

    void addTaskContext(shared_ptr<TaskContext> taskContext){
       this->taskContext = taskContext;
    }

    string getInfo() {
        return "broadCast buffer";
    }

    string getOutputBufferName()
    {
        return "BroadcastOutputBuffer";
    }

    vector<shared_ptr<DataPage>> getStoredPages(string bufferId,long token)
    {
        bool endFounded = false;
        int i = allPages.size() - 1;
        for( ; i >= 0 ; i--)
        {
            if(allPages[i]->isEndPage())
                break;
            else
                endFounded = true;
        }
        vector<shared_ptr<DataPage>> result;
        for(int j = 0 ; j <= i ; j++)
        {
            result.push_back(allPages[j]);
        }
        if(endFounded == true)
            this->buffers[bufferId]->enqueuePages({allPages[allPages.size() - 1]});

        return result;
    }

    vector<shared_ptr<DataPage>> getPages(string bufferId,long token) {

        vector<shared_ptr<DataPage>> result;

        if(this->buffers.count(bufferId) == 0)
        {
            // cout << "Fatal ERROR! BroadCastBuffer cannot find the bufferId "<< bufferId <<" !"<< endl;
            shared_ptr<ClientBuffer> cb = make_shared<ClientBuffer>(bufferId);

            this->buffers[bufferId] = cb;
            this->buffers[bufferId]->enqueuePages(allPages);


            return result;
        }
        else
        {
            result = buffers[bufferId]->getPages();
        }

        return result;

    }


    vector<shared_ptr<DataPage>> getPages(string bufferId,long token,int pageNums) {

        vector<shared_ptr<DataPage>> result;


        int bufferIdCount = this->buffers.count(bufferId);


        if(bufferIdCount == 0)
        {
            // cout << "Fatal ERROR! BroadCastBuffer cannot find the bufferId "<< bufferId <<" !"<< endl;
            lock.lock();
            shared_ptr<ClientBuffer> cb = make_shared<ClientBuffer>(bufferId);

            this->buffers[bufferId] = cb;
            this->buffers[bufferId]->enqueuePages(allPages);
            this->maxBufferId++;
            lock.unlock();
        }
        else
        {
            result = buffers[bufferId]->getPages(10000);

        }

        return result;

    }




    void enqueue(vector<shared_ptr<DataPage>> pages) {

        vector<shared_ptr<DataPage>> pagesToEnqueue;
        lock.lock();
        for(int i = 0 ; i < pages.size() ; i++) {


            if(pages[i]->isEndPage())
            {
                this->endPageNum++;
                if(this->outputOperatorCount == this->endPageNum) {
                    this->endPageFound = true;
                    allPages.push_back(pages[i]);
                    pagesToEnqueue.push_back(pages[i]);
                }
            }
            else {
                allPages.push_back(pages[i]);
                pagesToEnqueue.push_back(pages[i]);
            }
        }
        lock.unlock();



        for(auto buffer : buffers) {
            buffer.second->enqueuePages(pagesToEnqueue);
        }

    }
    void regOutputOperator(){
        this->outputOperatorCount++;
    }
    bool isFull() {

        //return false;

        for(auto buffer : buffers) {
            if(buffer.second->getPageNums() > pageNumsLimit)
            {
                return true;
            }
        }
        return false;
    }

    bool isEmpty()
    {
        return false;
    }
    void destoryPages()
    {
       this->allPages.clear();
    }

    void closeBuffer(string bufferId)
    {

    }

    void setOutputBuffersSchema(OutputBufferSchema schema){


        this->bufferSchema = make_shared<OutputBufferSchema>(schema.getBufferType(),schema.getBuffers());

        map<string,int> tBuffers = this->bufferSchema->getBuffers();

        for(auto buffer : tBuffers)
        {
            if(this->buffers.count(buffer.first) == 0)
            {
                shared_ptr<ClientBuffer> cb = make_shared<ClientBuffer>(buffer.first);
                this->buffers[buffer.first] = cb;
            }
        }

    }
};




#endif //OLVP_BROADCASTOUTPUTBUFFER_HPP
