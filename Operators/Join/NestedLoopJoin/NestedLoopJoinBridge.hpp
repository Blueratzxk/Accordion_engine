//
// Created by zxk on 6/28/23.
//

#ifndef OLVP_NESTEDLOOPJOINBRIDGE_HPP
#define OLVP_NESTEDLOOPJOINBRIDGE_HPP

#include "../JoinBridge.hpp"
#include "../../../Page/DataPage.hpp"
#include <future>
class NestedLoopJoinPages
{
    vector<shared_ptr<DataPage>> pages;

public:
    NestedLoopJoinPages(vector<shared_ptr<DataPage>> pages)
    {
        this->pages = pages;
    }

    vector<shared_ptr<DataPage>> getPages()
    {
        return this->pages;
    }
};


class NestedLoopJoinBridge:public JoinBridge
{
public:
    virtual std::future<shared_ptr<NestedLoopJoinPages>>  getPagesFuture() = 0;
    virtual void setPages(shared_ptr<NestedLoopJoinPages>) = 0;
    virtual void tryGetCompletedPages() = 0;

};
#endif //OLVP_NESTEDLOOPJOINBRIDGE_HPP
