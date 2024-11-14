//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_ARIBITARYEXCHANGER_HPP
#define OLVP_ARIBITARYEXCHANGER_HPP

#include "LocalExchanger.hpp"

class ArbitaryExchanger:public LocalExchanger
{

    mutex sourcesLock;
    list<std::shared_ptr<LocalExchangeSource>> sources;

    atomic<int> sinkCount = 0;
    atomic<int> endPageCount = 0;

    int loopIndex = 0;

    atomic<bool> finished = false;

    BlockQueue<shared_ptr<DataPage>> cachedPages;
    atomic<int> endCount = 0;

public:

    ArbitaryExchanger(list<std::shared_ptr<LocalExchangeSource>> sources)
    {

        this->sources = sources;
    }


    void addSource(std::shared_ptr<LocalExchangeSource> source)
    {
        this->sourcesLock.lock();
        if(!this->finished)
            this->sources.push_back(source);
        else
        {
            this->sources.push_back(source);
            source->addPage(DataPage::getEndPage());
        }
        this->sourcesLock.unlock();
    }

    int getTargetSource()
    {

        this->loopIndex++;
        if(this->loopIndex >= this->sources.size())
            this->loopIndex = 0;
        return this->loopIndex;
    }

    bool isFinished() override
    {
        return this->finished;
    }
    bool canExchange() override{



        int totalSize = 0;
        this->sourcesLock.lock();


        if(this->sources.empty())
        {
            this->sourcesLock.unlock();
            return false;
        }


        auto front = this->sources.begin();
        std::advance(front, this->getTargetSource());
        totalSize+=(*front)->getSourceSize();
        this->sourcesLock.unlock();


        if(totalSize > 200)
            return false;

        return true;

    }

    vector<shared_ptr<DataPage>> pageSpliter(std::shared_ptr<DataPage> page)
    {

        int splitCount = this->sources.size();
        int elementCount = page->getElementsCount();
        int pace = elementCount/splitCount;

        if(page->getElementsCount() < splitCount)
            return {page};

        vector<shared_ptr<DataPage>> splitedBatchs;
        for(int i = 0 ; i < splitCount ; i++)
        {
            if(i == splitCount - 1)
                splitedBatchs.push_back(make_shared<DataPage>(page->get()->Slice(i*pace,elementCount - i * pace)));
            else
                splitedBatchs.push_back(make_shared<DataPage>(page->get()->Slice(i*pace,pace)));
        }
        return splitedBatchs;

    }


    void closeSource(int number)
    {
        this->sourcesLock.lock();

     //   spdlog::debug("sinkCount! "+ to_string(this->sinkCount) + "endpageCount" + to_string(this->endPageCount) +"source size " +
     //                         to_string(this->sources.size())+ "status:" + to_string(this->finished));

        if(this->endCount < this->sources.size() - 1) {
            this->sources.back()->addPage(DataPage::getEndPage());
            this->endCount++;

        }
        this->sourcesLock.unlock();
    }

    void accept(std::shared_ptr<DataPage> page)
    {
        this->sourcesLock.lock();
        if(!page->isEndPage()) {

            this->sources.remove_if([](std::shared_ptr<LocalExchangeSource> source){return source->isEnd();});


            vector<std::shared_ptr<LocalExchangeSource>> addrs;
            auto front = this->sources.begin();
            for(int i = 0 ; i < this->sources.size() ; i++) {
                addrs.push_back(*front);
                std::advance(front, 1);
            }


            //std::advance(Frontend, this->getTargetSource());
            vector<shared_ptr<DataPage>> splitedPages = pageSpliter(page);
            for(int i = 0 ; i < splitedPages.size() ; i++) {

                (addrs[i])->addPage(splitedPages[i]);
            }
        }
        else
            this->endPageCount++;

        if(this->endPageCount == sinkCount)
        {
            this->finished = true;
            for (auto source: this->sources) {

                source->addPage(DataPage::getEndPage());
            }
        }
        this->sourcesLock.unlock();


    }

    void declareSinkCount()
    {
        this->sourcesLock.lock();
        this->sinkCount++;
        this->sourcesLock.unlock();
    }



};


#endif //OLVP_ARIBITARYEXCHANGER_HPP
