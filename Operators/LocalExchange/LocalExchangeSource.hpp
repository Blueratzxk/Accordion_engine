//
// Created by zxk on 5/27/23.
//

#ifndef OLVP_LOCALEXCHANGESOURCE_HPP
#define OLVP_LOCALEXCHANGESOURCE_HPP

#include "tbb/concurrent_queue.h"
#include "../../Page/DataPage.hpp"
#include "../../Utils/BlockQueue.hpp"
class LocalExchangeSource
{

    int id;
    std::shared_ptr<tbb::concurrent_queue<std::shared_ptr<DataPage>>> sourceBuffer;

    mutex sizeLock;

    atomic<bool> end = false;

public:
    LocalExchangeSource()
    {
        this->sourceBuffer = std::make_shared<tbb::concurrent_queue<std::shared_ptr<DataPage>>>();


    }

    bool isEnd()
    {
        return this->end;
    }


    LocalExchangeSource(int id,std::shared_ptr<tbb::concurrent_queue<std::shared_ptr<DataPage>>> sourceBuffer)
    {
        this->id = id;
        this->sourceBuffer = sourceBuffer;
    }

    void addPage(std::shared_ptr<DataPage> page)
    {
        this->sourceBuffer->push(page);
    }

    std::shared_ptr<DataPage> removePage()
    {
        std::shared_ptr<DataPage> out = NULL;
        this->sourceBuffer->try_pop(out);
        //spdlog::debug("LocalExchangeSource:"+to_string(this->id)+" get page from shared buffer!");
        if(out != NULL && out->isEndPage())
            this->end = true;
        return out;

    }
    int getSourceSize()
    {
        sizeLock.lock();
        int size = this->sourceBuffer->unsafe_size();
        sizeLock.unlock();
        return size;
    }

    bool empty()
    {
        return this->sourceBuffer->empty();
    }


};



#endif //OLVP_LOCALEXCHANGESOURCE_HPP
