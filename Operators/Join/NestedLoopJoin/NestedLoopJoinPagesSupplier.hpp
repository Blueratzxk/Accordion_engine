//
// Created by zxk on 6/28/23.
//

#ifndef OLVP_NESTEDLOOPJOINPAGESSUPPLIER_HPP
#define OLVP_NESTEDLOOPJOINPAGESSUPPLIER_HPP

#include "NestedLoopJoinBridge.hpp"

class NestedLoopJoinPagesSupplier:public NestedLoopJoinBridge
{

    vector<shared_ptr<std::promise<shared_ptr<NestedLoopJoinPages>>>> promisePages;
    shared_ptr<NestedLoopJoinPages> buildPages = NULL;

    mutex lock;

public:

    std::future<shared_ptr<NestedLoopJoinPages>> getPagesFuture() override
    {
        lock.lock();

        auto promise = std::make_shared<std::promise<shared_ptr<NestedLoopJoinPages>>>();
        this->promisePages.push_back(promise);

        lock.unlock();

        return promise->get_future();
    }

    void setPages(shared_ptr<NestedLoopJoinPages> pages) override
    {
        lock.lock();
        this->buildPages = pages;
        for(int i = 0 ; i < this->promisePages.size() ; i++)
        {
            promisePages[i]->set_value(pages);
        }
        this->promisePages.clear();
        lock.unlock();
    }
    void tryGetCompletedPages()
    {
        if(this->buildPages == NULL)
            return;

        lock.lock();
        for(int i = 0 ; i < this->promisePages.size() ; i++)
        {
            promisePages[i]->set_value(this->buildPages);
        }
        this->promisePages.clear();
        lock.unlock();
    }


};



#endif //OLVP_NESTEDLOOPJOINPAGESSUPPLIER_HPP
