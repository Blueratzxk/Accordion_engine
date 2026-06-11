//
// Created by zxk on 6/10/26.
//

#ifndef OLVP_BLOCKCLIENTBUFFER_HPP
#define OLVP_BLOCKCLIENTBUFFER_HPP
#include "../../Page/DataPage.hpp"
#include "tbb/concurrent_queue.h"

#include "../Event/SimpleEvent.hpp"
class BlockClientBuffer
{

    string bufferId;
    std::deque<shared_ptr<DataPage>> pages;
    atomic<bool> findEndPage = false;
    atomic<bool> bufferIsEmpty;
    shared_ptr<SimpleEvent> simpleEvent;
    mutex lock;


public:
    BlockClientBuffer(string bufferId){
        this->bufferId = bufferId;
        this->findEndPage = false;
        this->simpleEvent = make_shared<SimpleEvent>();
    }
    bool endPageFound()
    {
        return this->findEndPage;
    }
    void enqueuePages(vector<shared_ptr<DataPage>> inputPages)
    {
        lock.lock();
        if(!inputPages.empty() && inputPages[0]->isEndPage())
        {
            spdlog::info("####enqueue end page!!!#######");
        }

        for(int i = 0 ; i < inputPages.size() ; i++) {
            this->pages.push_back(inputPages[i]);
        }
        lock.unlock();
    }
    vector<shared_ptr<DataPage>> getPages()
    {
        vector<shared_ptr<DataPage>> result;

        lock.lock();
        if(!this->pages.empty()) {
            while (true) {
                if (this->pages.empty())
                    break;
                shared_ptr<DataPage> resultPage = this->pages.front();
                this->pages.pop_front();


                if (!resultPage->isEndPage()) {
                    result.push_back(resultPage);
                } else {
                    this->pages.push_back(resultPage);
                    if (this->findEndPage == false) {
                        this->findEndPage = true;
                        break;
                    }
                }

                if (this->findEndPage == true) {
                    resultPage = this->pages.front();
                    this->pages.pop_front();

                    lock.unlock();
                    return {resultPage};
                }
            }
        }
        else if(this->findEndPage == true)
        {
            lock.unlock();
            return {DataPage::getEndPage()};
        }

        lock.unlock();

        return result;
    }

    vector<shared_ptr<DataPage>> getPages(int pageNums)
    {
        vector<shared_ptr<DataPage>> result;

        int i = 0 ;

        lock.lock();

        if(!this->pages.empty()) {
            while (true) {
                if (this->pages.empty())
                    break;
                shared_ptr<DataPage> resultPage = this->pages.front();
                this->pages.pop_front();

                if (!resultPage->isEndPage()) {
                    result.push_back(resultPage);
                    i++;
                    if (i >= pageNums)
                        break;
                } else {
                    this->pages.push_back(resultPage);
                    if (this->findEndPage == false) {
                        this->findEndPage = true;
                        break;
                    }
                }

                if (this->findEndPage == true) {
                    resultPage = this->pages.front();
                    this->pages.pop_front();
                    lock.unlock();
                    return {resultPage};
                }
            }
        }
        else if(this->findEndPage == true)
        {
            lock.unlock();
            return {DataPage::getEndPage()};
        }

        lock.unlock();
        return result;
    }

    size_t getPageNums()
    {
        lock.lock();
        int size = this->pages.size();
        lock.unlock();
        return size;
    }
    void clear()
    {
        lock.lock();
        this->pages.clear();
        lock.unlock();
    }
};

#endif //OLVP_BLOCKCLIENTBUFFER_HPP
