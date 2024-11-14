//
// Created by zxk on 6/27/24.
//

#ifndef OLVP_HASHEXCHANGER_HPP
#define OLVP_HASHEXCHANGER_HPP



#include "LocalExchanger.hpp"
#include "../../Execution/Buffer/OutputPartitioningBuffer/BufferShuffler/SimplePageHashGenerator.hpp"
#include "../../Page/DataPageBuilder.hpp"
#include "../../Execution/Buffer/OutputPartitioningBuffer/BufferShuffler/HashShuffler.hpp"

class hashExchanger:public LocalExchanger
{

    class ShuffleExecutor
    {
        hashExchanger *exchanger;


    public:
        ShuffleExecutor(hashExchanger *exchanger){
            this->exchanger = exchanger;


        }

        hashExchanger *getExchanger()
        {
            return this->exchanger;
        }




        static void releaseShuffleExecutor(shared_ptr<ShuffleExecutor> shuffleExecutor) {
            thread(shuffler, shuffleExecutor->getExchanger()).detach();
        }
        static void shuffler(hashExchanger *exchanger){
            int pageCounter = 0;


            exchanger->shuffleExecutorNums++;
            while(true)
            {
                wait:

                shared_ptr<DataPage> pageGet = exchanger->cachedPages.Take();

                if(pageGet->isShuffleExecutorExitPage())
                    return;



                exchanger->activeShuffleExecutorNumber++;

                if (pageGet->isEndPage()) {

                    exchanger->endPageCount++;
                    if(exchanger->endPageCount == exchanger->sinkCount)
                    {
                        if(!exchanger->closureThread)
                            exchanger->closureThread = true;
                        else {
                            exchanger->activeShuffleExecutorNumber--;
                            spdlog::info("Thread process "+ to_string(pageCounter) + " pages!");
                            goto wait;
                        }

                        while(exchanger->activeShuffleExecutorNumber > 1);

                        exchanger->finished = true;
                        for (auto source: exchanger->sources) {

                            source->addPage(DataPage::getEndPage());
                        }

                        for(int i = 0 ; i < exchanger->shuffleExecutorNums ; i++)
                            exchanger->cachedPages.Put(DataPage::getShuffleExecutorExitPage());
                    }
                    exchanger->activeShuffleExecutorNumber--;
                    spdlog::info("Thread process "+ to_string(pageCounter) + " pages!");





                }
                else {

                    HashShuffler localShuffler(exchanger->hashChannels);
                    vector<map<int, shared_ptr<DataPage>>> ShuffledPages;
                    localShuffler.processOptimize(exchanger->sources.size(), pageGet);
                    ShuffledPages.push_back(localShuffler.buildMap());

                    int i = 0 ;
                    for (auto source: exchanger->sources) {
                        if(ShuffledPages[0].count(i) > 0)
                            source->addPage(ShuffledPages[0][i]);
                        i++;
                    }
                    (pageCounter)++;
                    exchanger->activeShuffleExecutorNumber--;
                }

            }
        }

    };

    mutex sourcesLock;
    list<std::shared_ptr<LocalExchangeSource>> sources;

    atomic<int> sinkCount = 0;
    atomic<int> endPageCount = 0;

    int loopIndex = 0;

    atomic<bool> finished = false;

    BlockQueue<shared_ptr<DataPage>> cachedPages;
    atomic<int> endCount = 0;

    atomic<int> partitionedCount = 0;
    vector<int> hashChannels;

    atomic<int> activeShuffleExecutorNumber = 0;

    bool shuffleExecutorMode = true;
    shared_ptr<ShuffleExecutor> shuffleExecutors;
    atomic<bool> closureThread = false;

    vector<int> threadIDs;

    int shuffleExecutorNums = 0;

public:

    shared_ptr<DataPage> getCachedPage()
    {
        return this->cachedPages.Take();
    }



    hashExchanger(list<std::shared_ptr<LocalExchangeSource>> sources,vector<int> hashChannels)
    {

        this->sources = sources;
        this->hashChannels = hashChannels;
        this->partitionedCount = this->sources.size();

        if(this->shuffleExecutorMode)
        {
            this->shuffleExecutors = make_shared<ShuffleExecutor>(this);
            for(int i = 0 ; i < 16 ; i++)
                ShuffleExecutor::releaseShuffleExecutor(this->shuffleExecutors);
        }
    }


    void addSource(std::shared_ptr<LocalExchangeSource> source)
    {
        /*
        this->sourcesLock.lock();
        if(!this->finished) {
            this->sources.push_back(source);
            this->partitionedCount++;
        }
        else
        {
            this->sources.push_back(source);
            source->addPage(DataPage::getEndPage());
        }
        this->sourcesLock.unlock();
         */
        spdlog::warn("hashExchanger cannot add extra sources!");
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

        return true;
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
       spdlog::warn("Hash exchanger cannot close source!");
    }

    shared_ptr<map<int,shared_ptr<DataPage>>>  processOptimize(int partitionCount,shared_ptr<DataPage> page)
    {

        map<int,vector<int>> partitionAssignment;
        shared_ptr<SimplePageHashGenerator> hashGen = make_shared<SimplePageHashGenerator>(this->hashChannels);
        map<int,shared_ptr<DataPageBuilder>> partitionedPage;


        for(int i = 0 ; i < page->getElementsCount() ; i++) {
            int partitionNumber = hashGen->getPartition(partitionCount, i, page);
            partitionAssignment[partitionNumber].push_back(i);
        }

        for(auto partition : partitionAssignment)
        {
            partitionedPage[partition.first] = make_shared<DataPageBuilder>(page->get()->schema());
            partitionedPage[partition.first]->appendRows(partition.second,page);
        }


        shared_ptr<map<int,shared_ptr<DataPage>>> resultMap = make_shared<map<int,shared_ptr<DataPage>>>();

        for(auto pages : partitionedPage)
        {
            (*resultMap)[pages.first] = (pages.second->build());
            pages.second = NULL;
        }

        //     for(auto page : *resultMap)
        //       {
        //         spdlog::info(page.second->get()->ToString());
        //     }

        return resultMap;
    }


    shared_ptr<map<int,shared_ptr<DataPage>>>  process(int partitionCount,shared_ptr<DataPage> page)
    {

        map<int,vector<int>> partitionAssignment;

        shared_ptr<SimplePageHashGenerator> hashGen = make_shared<SimplePageHashGenerator>(this->hashChannels);
        map<int,shared_ptr<DataPageBuilder>> partitionedPage;

        for(int i = 0 ; i < page->getElementsCount() ; i++)
        {
            int partitionNumber = hashGen->getPartition(partitionCount,i,page);

            if(partitionedPage.count(partitionNumber) == 0)
            {
                partitionedPage[partitionNumber] = make_shared<DataPageBuilder>(page->get()->schema());
                partitionedPage[partitionNumber]->appendRow(i,page);
            }
            else
                partitionedPage[partitionNumber]->appendRow(i,page);

        }
        shared_ptr<map<int,shared_ptr<DataPage>>> resultMap = make_shared<map<int,shared_ptr<DataPage>>>();

        for(auto pages : partitionedPage)
        {
            (*resultMap)[pages.first] = (pages.second->build());
            pages.second = NULL;
        }

   //     for(auto page : *resultMap)
   //       {
    //         spdlog::info(page.second->get()->ToString());
    //     }

        return resultMap;
    }



    void accept(std::shared_ptr<DataPage> page)
    {

        if(this->shuffleExecutorMode)
        {
          //  auto pageSplits = pageSpliter(page);

          //  for(auto page : pageSplits){
                this->cachedPages.Put(page);
         //   }
            return;
        }


        auto re = this->processOptimize(this->partitionedCount,page);

        if(!page->isEndPage()) {

            int i = 0 ;
            for (auto source: this->sources) {
                if((*re).count(i) > 0)
                    source->addPage((*re)[i]);
                i++;
            }
            

        }
        else {
        //   stringstream stream;
        //    stream << this_thread::get_id();
        //    string pidStr;
        //    stream >> pidStr;
        //    spdlog::info("Thread "+pidStr+" exit!");
            this->endPageCount++;
        }

        if(this->endPageCount == sinkCount)
        {
            this->finished = true;
            for (auto source: this->sources) {

                source->addPage(DataPage::getEndPage());
            }
        }


    }

    void declareSinkCount()
    {
        this->sourcesLock.lock();
        this->sinkCount++;
        this->sourcesLock.unlock();
    }



};




#endif //OLVP_HASHEXCHANGER_HPP
