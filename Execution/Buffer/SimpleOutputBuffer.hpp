//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SIMPLEOUTPUTBUFFER_HPP
#define OLVP_SIMPLEOUTPUTBUFFER_HPP


#include <atomic>
//#include "../../common.h"
#include "OutputBuffer.hpp"
#include "ClientBuffer.hpp"
#include "OutputBufferSchema.hpp"
#include "tbb/concurrent_map.h"


class SimpleOutputBuffer: public OutputBuffer
{

    atomic<int> pageNumsLimit = 1;
    int maxPageNumsLimit = 1000;
    shared_ptr<OutputBufferSchema> bufferSchema = OutputBufferSchema::createInitialEmptyOutputBufferSchema(OutputBufferSchema::BufferType::SIMPLE);
    shared_ptr<ClientBuffer> oneBuffer = make_shared<ClientBuffer>("0");


    atomic<bool> endPageFounded = false;


    tbb::concurrent_map<string,bool> bufferIdToEndPage;

    int loopIndex = 0;

    int sizeForChange = 10;

    shared_ptr<TaskContext> taskContext = NULL;

    atomic<long> remainingTuples = 0;

    enum tuningRoles {None,consumer,producer};
    std::atomic<tuningRoles> curRole = None;
    int expandTime = 0;



    atomic<int> outputOperatorCount = 0;
    atomic<int> endPageNum = 0;


    int traffic = 0;
    shared_ptr<std::chrono::system_clock::time_point> start = NULL;
    int bufferTuneCircle = 500; //ms

    mutex timelock;


public:

    SimpleOutputBuffer(OutputBufferSchema schema){
        this->setOutputBuffersSchema(schema);
    }

    string getInfo() {
        return "simple buffer";
    }
    string getOutputBufferName()
    {
        return "SimpleOutputBuffer";
    }

    void regOutputOperator(){
        this->outputOperatorCount++;
    }
    void addTaskContext(shared_ptr<TaskContext> taskContext){
        this->taskContext = taskContext;
    }


    vector<shared_ptr<DataPage>> getPages(string bufferId,long token) {

        vector<shared_ptr<DataPage>> result;
        result = oneBuffer->getPages();

        for(auto re :result) {
            this->remainingTuples -= re->getElementsCount();

            if (this->taskContext != NULL)
                this->taskContext->updateRemainingBufferTupleCount(-re->getElementsCount());
        }

        tuneBufferCapacity("consumer");
        this->traffic += result.size();
        this->resetBufferSizeByTrafficRate();
        return result;

    }


    vector<shared_ptr<DataPage>> getPages(string bufferId,long token,int pageNums) {

        vector<shared_ptr<DataPage>> result;


        if(this->bufferIdToEndPage.count(bufferId) == 0)
            this->bufferIdToEndPage[bufferId] = false;

        if (this->endPageFounded) {
            //  this->buffers[bufferId]->enqueuePages({this->EndPageAddr});

            if(this->bufferIdToEndPage.count(bufferId) > 0) {
                if(bufferIdToEndPage[bufferId] == false) {
                    this->oneBuffer->enqueuePages({DataPage::getEndPage()});
                    bufferIdToEndPage[bufferId] = true;
                }
                else
                {
                    this->oneBuffer->enqueuePages({DataPage::getEndPage()});
                }
            }

        }
        else
        {
            if(this->bufferIdToEndPage.count(bufferId) > 0)//we don't find end page,but the buffer is stopped,this is an early stop
            {
                if(this->bufferIdToEndPage[bufferId] == true) {
                    result.push_back(DataPage::getEndPage());
                    return result;
                }

            }
        }

        result = this->oneBuffer->getPages(pageNums);

        for(auto re :result) {
            this->remainingTuples -= re->getElementsCount();
            if(taskContext!=NULL)
                this->taskContext->updateRemainingBufferTupleCount(-re->getElementsCount());
        }

        //spdlog::info("All:"+ to_string(this->oneBuffer->getPageNums())+"Request:"+to_string(pageNums)+" Get:"+ to_string(result.size()));

        tuneBufferCapacity("consumer");
        this->traffic += result.size();
        this->resetBufferSizeByTrafficRate();

        return result;

    }

    void resetBufferSizeByTrafficRate()
    {
        timelock.lock();
        if (this->start == NULL) {

            this->start = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());

            this->traffic = 0;
        }else {
            shared_ptr<std::chrono::system_clock::time_point> circle = make_shared<std::chrono::system_clock::time_point>(std::chrono::system_clock::now());


            double duration_millsecond = std::chrono::duration<double, std::milli>(*circle - *this->start).count();

            if(duration_millsecond > bufferTuneCircle)
            {
                if(this->traffic > 0)
                    this->pageNumsLimit = this->traffic;
                else
                    this->pageNumsLimit = 1;

                if(this->traffic < this->pageNumsLimit)
                    if(this->taskContext != NULL)
                        this->taskContext->addBufferSizeTurnDownCounter();


                this->start = NULL;

            }
        }
        timelock.unlock();
    }

    void expandBufferCapacity()
    {
        this->pageNumsLimit+=5;
        if(this->pageNumsLimit >= 1000)
            this->pageNumsLimit = 1000;

        spdlog::debug("expand pageNumsLimit!"+to_string(pageNumsLimit));
    }

    void reduceBufferCapacity()
    {
        this->pageNumsLimit-=5;
        if(this->pageNumsLimit <= 5)
            this->pageNumsLimit = 5;
        spdlog::debug("reduce pageNumsLimit!"+to_string(pageNumsLimit));
    }


    void tuneBufferCapacity(string role)
    {
        if(role == "producer" && this->curRole == producer)
            return;
        if(role == "consumer" && this->curRole == consumer)
            return;

        if(role == "producer" && this->isFull()) {
            this->curRole = producer;
            this->reduceBufferCapacity();
         //   this->taskContext->addBufferSizeTurnDownCounter();
            this->expandTime = 0;
        }

        if(role == "consumer" && this->isEmpty()) {
            this->curRole = consumer;
        //    if(this->expandTime < 3) {
                this->expandBufferCapacity();
            if(this->taskContext != NULL)
                this->taskContext->addBufferSizeTurnUpCounter();
                this->expandTime++;
        //    }

        }

        if(role == "producer")
            this->curRole = producer;
        if(role == "consumer")
            this->curRole = consumer;


    }


    void enqueue(vector<shared_ptr<DataPage>> pages) {


        vector<shared_ptr<ClientBuffer>> client_buffers;

        for(int i = 0 ; i < pages.size() ; i++)
        {

            if(pages[i]->isEndPage()) {
                this->endPageNum++;
                if(this->endPageNum == this->outputOperatorCount) {
                    this->endPageFounded = true;
                    this->oneBuffer->enqueuePages({pages[i]});
                }
            }
            else
            {
                this->oneBuffer->enqueuePages({pages[i]});
                this->remainingTuples += pages[i]->getElementsCount();
                if(this->taskContext != NULL)
                    this->taskContext->updateRemainingBufferTupleCount(pages[i]->getElementsCount());
            }
        }

        if(this->taskContext != NULL)
            this->taskContext->setLastEnqueuedTupleCount(this->remainingTuples);

        tuneBufferCapacity("producer");

    }
    bool isFull() {

        if(this->endPageFounded)
            return false;

        if(this->oneBuffer->getPageNums() >= this->pageNumsLimit)
            return true;
        else
            return false;
    }
    bool isEmpty()
    {
        return this->oneBuffer->getPageNums() == 0;
    }

    void changeBufferSize()
    {
        /*
        this->pageNumsLimit+=sizeForChange;
        if(this->pageNumsLimit <=0)
            this->pageNumsLimit = 1;
        if(this->pageNumsLimit > this->maxPageNumsLimit)
            this->pageNumsLimit = maxPageNumsLimit;
        spdlog::debug("SimpleOutputBuffer size is changed! Now the size is "+ to_string(this->pageNumsLimit)+"!");
         */
    }

    void destoryPages()
    {

    }

    void closeBuffer(string bufferId)
    {

        if(this->bufferIdToEndPage.count(bufferId) > 0)
            this->bufferIdToEndPage[bufferId] = true;
    }


    void setOutputBuffersSchema(OutputBufferSchema schema){

    }
};


#endif //OLVP_SIMPLEOUTPUTBUFFER_HPP
