//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKSIMPLEOUTPUTBUFFER_HPP
#define OLVP_INTERTASKSIMPLEOUTPUTBUFFER_HPP

#include <atomic>
//#include "../../common.h"
#include "OutputBuffer.hpp"
#include "BlockClientBuffer.hpp"
#include "OutputBufferSchema.hpp"
#include "tbb/concurrent_map.h"

class InterTaskSimpleOutputBuffer: public OutputBuffer
{

    atomic<int> pageNumsLimit = 1;
    int maxPageNumsLimit = 1000;
    shared_ptr<OutputBufferSchema> bufferSchema = OutputBufferSchema::createInitialEmptyOutputBufferSchema(OutputBufferSchema::BufferType::SIMPLE);
    shared_ptr<BlockClientBuffer> oneBuffer = make_shared<BlockClientBuffer>("0");


    atomic<bool> endPageFounded = false;


    tbb::concurrent_map<string,bool> bufferIdToEndPage;

    int loopIndex = 0;

    int sizeForChange = 10;


    atomic<long> remainingTuples = 0;

    enum tuningRoles {None,consumer,producer};
    std::atomic<tuningRoles> curRole = None;
    int expandTime = 0;



    atomic<int> endPageNum = 0;


    int traffic = 0;
    shared_ptr<std::chrono::system_clock::time_point> start = NULL;
    int bufferTuneCircle = 500; //ms

    mutex timelock;

    std::deque<std::vector<std::shared_ptr<DataPage>>> groupedPages;


public:

    InterTaskSimpleOutputBuffer(OutputBufferSchema schema){
        this->setOutputBuffersSchema(schema);
    }

    InterTaskSimpleOutputBuffer(){
    }

    string getInfo() {
        return "simple buffer";
    }
    string getOutputBufferName()
    {
        return "SimpleOutputBuffer";
    }

    std::deque<std::vector<std::shared_ptr<DataPage>>>
    GroupByAdjacentSchema(
        const std::vector<std::shared_ptr<DataPage>>& pages) {

        std::deque<std::vector<std::shared_ptr<DataPage>>> result;

        if (pages.empty()) {
            return result;
        }

        if (pages[0]->isEndPage()) {
            result.push_front({pages[0]});
            return result;
        }
        auto current_schema = pages[0]->get()->schema();
        std::vector<std::shared_ptr<DataPage>> group;
        group.reserve(1024);

        for (const auto& page : pages) {

            auto schema = page->get()->schema();

            // schema变化 -> flush上一组
            if (!schema->Equals(*current_schema)) {
                result.push_front(std::move(group));
                group.clear();
                current_schema = schema;
            }

            group.push_back(page);
        }

        // flush最后一组
        if (!group.empty()) {
            result.push_front(std::move(group));
        }

        return result;
    }

    vector<shared_ptr<DataPage>> getPages(string bufferId,long token) {

        vector<shared_ptr<DataPage>> result;
        result = oneBuffer->getPages();

        for(auto re :result) {
            this->remainingTuples -= re->getElementsCount();

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


        result = this->oneBuffer->getPages();

        for(auto re :result) {
            this->remainingTuples -= re->getElementsCount();

        }

        //spdlog::info("All:"+ to_string(this->oneBuffer->getPageNums())+"Request:"+to_string(pageNums)+" Get:"+ to_string(result.size()));

        tuneBufferCapacity("consumer");
        this->traffic += result.size();
        this->resetBufferSizeByTrafficRate();

        return result;

    }

    vector<shared_ptr<DataPage>> getPagesBySchema(string bufferId,long token) {

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

        if (this->groupedPages.empty()) {
            result = this->oneBuffer->getPages();
            this->groupedPages = this->GroupByAdjacentSchema(result);
        }

        spdlog::warn("+++++++");
        for (auto group : this->groupedPages) {
            spdlog::warn("--------");
            string schemas;
            int elememts = 0;
            for (auto page : group) {
                elememts += page->getElementsCount();
            }
            if (!group.empty() && !group[0]->isEndPage()) {
                schemas += group[0]->get()->schema()->ToString();
                schemas+="|";
                schemas+=to_string(group.size());
                schemas+="|";
                schemas += to_string(elememts);
                spdlog::warn(schemas);
            }
            spdlog::warn("--------");
        }
        spdlog::warn("+++++++++");

        result = this->groupedPages.back();
        this->groupedPages.pop_back();

        for(auto re :result) {
            this->remainingTuples -= re->getElementsCount();

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

            this->expandTime++;
            //    }

        }

        if(role == "producer")
            this->curRole = producer;
        if(role == "consumer")
            this->curRole = consumer;


    }


    void enqueue(vector<shared_ptr<DataPage>> pages) {


        vector<shared_ptr<BlockClientBuffer>> client_buffers;

        vector<shared_ptr<DataPage>> allPagesToSend;
        for(int i = 0 ; i < pages.size() ; i++)
        {

            if(pages[i]->isEndPage()) {
                this->endPageNum++;

                this->endPageFounded = true;
                allPagesToSend.push_back(pages[i]);

            }
            else
            {
                allPagesToSend.push_back(pages[i]);

                this->remainingTuples += pages[i]->getElementsCount();

            }
        }
        this->oneBuffer->enqueuePages(allPagesToSend);


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



#endif //OLVP_INTERTASKSIMPLEOUTPUTBUFFER_HPP
