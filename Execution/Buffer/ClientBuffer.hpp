//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_CLIENTBUFFER_HPP
#define OLVP_CLIENTBUFFER_HPP

//#include "../../common.h"
#include "../../Page/DataPage.hpp"
#include "tbb/concurrent_queue.h"

#include "../Event/SimpleEvent.hpp"
class ClientBuffer
{

    string bufferId;
    tbb::concurrent_queue<shared_ptr<DataPage>> pages;
    atomic<bool> findEndPage = false;
    atomic<bool> bufferIsEmpty;
    shared_ptr<SimpleEvent> simpleEvent;


public:
    ClientBuffer(string bufferId){
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



        if(!inputPages.empty() && inputPages[0]->isEndPage())
        {
            spdlog::info("####enqueue end page!!!#######");
        }

        for(int i = 0 ; i < inputPages.size() ; i++) {
            this->pages.push(inputPages[i]);
        }


    }
    vector<shared_ptr<DataPage>> getPages()
    {
        vector<shared_ptr<DataPage>> result;


        if(!this->pages.empty()) {
            while (true) {
                if (this->pages.empty())
                    break;
                shared_ptr<DataPage> resultPage;
                while (!this->pages.try_pop(resultPage));

                if (!resultPage->isEndPage()) {
                    result.push_back(resultPage);
                } else {
                    this->pages.push(resultPage);
                    if (this->findEndPage == false) {
                        this->findEndPage = true;
                        break;
                    }
                }

                if (this->findEndPage == true) {
                    while (!this->pages.try_pop(resultPage));
                    return {resultPage};
                }
            }
        }
        else if(this->findEndPage == true)
        {
            return {DataPage::getEndPage()};
        }


        return result;
    }

    vector<shared_ptr<DataPage>> getPages(int pageNums)
    {
        vector<shared_ptr<DataPage>> result;
       
        int i = 0 ;



        if(!this->pages.empty()) {
            while (true) {
                if (this->pages.empty())
                    break;
                shared_ptr<DataPage> resultPage;
                while (!this->pages.try_pop(resultPage));

                if (!resultPage->isEndPage()) {
                    result.push_back(resultPage);
                    i++;
                    if (i >= pageNums)
                        break;
                } else {
                    this->pages.push(resultPage);
                    if (this->findEndPage == false) {
                        this->findEndPage = true;
                        break;
                    }
                }

                if (this->findEndPage == true) {
                    while (!this->pages.try_pop(resultPage));
                    return {resultPage};
                }
            }
        }
        else if(this->findEndPage == true)
        {
            return {DataPage::getEndPage()};
        }



        return result;
    }

    size_t getPageNums()
    {
        return this->pages.unsafe_size();
    }
    void clear()
    {
        this->pages.clear();
    }
};


#endif //OLVP_CLIENTBUFFER_HPP
