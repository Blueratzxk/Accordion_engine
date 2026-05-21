//
// Created by zxk on 2/3/26.
//

#ifndef OLVP_SHUFFLECACHE_HPP
#define OLVP_SHUFFLECACHE_HPP

#include "BufferShuffler/HashShuffler.hpp"
class ShuffleCache
{
    int initialPartitionNums = 24;
    int initialExecutorNums = 4;

    mutex tidsLock;
    mutex tidSetShufflerLock;
    set<__pid_t> shuffleExecutorsTids;
    int shuffleExecutorNums = 0;

    atomic<int> executorTidReportNum = 0;
    atomic<int> activeShuffleExecutorNumber = 0;
    bool endPageFound = false;
    shared_ptr<DataPage> endPageAddr;
    shared_ptr<HashShuffler> shuffler;
    tbb::concurrent_map<string,shared_ptr<ClientBuffer>> buffers;

    int partitionCount = initialPartitionNums;

    vector<int> hashColumns ;

    shared_ptr<BlockQueue<shared_ptr<DataPage>>> unProcessedPages = make_shared<BlockQueue<shared_ptr<DataPage>>>();


    class ShuffleCacheExecutor {
        ShuffleCache *sc;
    public:
         ShuffleCacheExecutor(ShuffleCache *sc){
            this->sc = sc;
        }

        ShuffleCache * getsc()
        {
            return this->sc;
        }


        static void releaseShuffleExecutor(shared_ptr<ShuffleCacheExecutor> shuffleExecutor) {
            ++shuffleExecutor->getsc()->executorTidReportNum;
            thread th(shuffler,shuffleExecutor->getsc());
            th.detach();
        }
        static void shuffler(ShuffleCache * pb){

            pb->shuffleExecutorNums++;
            __pid_t id = gettid();

            pb->tidSetShufflerLock.lock();
            pb->shuffleExecutorsTids.insert(id);
            pb->tidSetShufflerLock.unlock();
            --pb->executorTidReportNum;

            while(true)
            {
                wait:

                shared_ptr<DataPage> pageGet = pb->unProcessedPages->Take();

                if(pageGet->isShuffleExecutorExitPage()) {
                    pb->shuffleExecutorNums--;
                    return;
                }


                pb->activeShuffleExecutorNumber++;


                if (pageGet->isEndPage()) {

                    while(pb->activeShuffleExecutorNumber > 1);

                    for(int i = 0 ; i < pb->shuffleExecutorNums ; i++)
                        pb->unProcessedPages->Put(DataPage::getShuffleExecutorExitPage());

                    pb->endPageFound = true;
                    pb->endPageAddr = pageGet;


                    for(int j = 0 ; j < pb->partitionCount ; j++)
                    {
                        pb->enqueueCachePages(to_string(j),{pb->endPageAddr});
                        spdlog::debug(to_string(long(pb))+to_string(j) + " add end page!");

                    }


                }
                else {

                    HashShuffler localShuffler(pb->hashColumns);
                    vector<map<int, shared_ptr<DataPage>>> ShuffledPages;
                    localShuffler.processOptimize(pb->partitionCount, pageGet);
                    ShuffledPages.push_back(localShuffler.buildMap());
                    for (auto pageMap: ShuffledPages) {
                        for (auto page: pageMap) {
                            pb->enqueueCachePages(to_string(page.first),{page.second});
                        }
                    }
                }

                --pb->activeShuffleExecutorNumber;
            }
        }
    };

    shared_ptr<ShuffleCacheExecutor> shuffleExecutors;

public:

    ShuffleCache(vector<int> hashColumns) {

        this->hashColumns = hashColumns;
        this->shuffleExecutors = make_shared<ShuffleCacheExecutor>(this);
        if(initialExecutorNums > 0)
        {
            tidsLock.lock();
            int actualNum = initialExecutorNums;

            for(int i = 0 ; i < actualNum ; i++)
                ShuffleCacheExecutor::releaseShuffleExecutor(this->shuffleExecutors);

            while(this->executorTidReportNum > 0) {}

            tidsLock.unlock();
        }
    }


    bool canRedistribute(int newPartitionCount) {

        auto re = this->partitionCount % newPartitionCount;
        if (re == 0)
            return true;
        return false;
    }

    map<string,vector<shared_ptr<DataPage>>> getRedistributionResult(int newPartitionCount) {
        map<string,vector<shared_ptr<DataPage>>> results;

        if (!canRedistribute(newPartitionCount))
            return results;

        for (int newPartitionIndex = 0 ; newPartitionIndex < newPartitionCount ; newPartitionIndex++) {
            for (int index = newPartitionIndex ; index < this->partitionCount ; index+=newPartitionCount) {

                auto result = this->buffers[to_string(index)]->getPages();
                for (auto re : result)
                    results[to_string(newPartitionIndex)].push_back(re);
            }
        }
        return results;
    }

    void enqueueCachePages(string bufferId,vector<shared_ptr<DataPage>> pages) {

        if (this->buffers.find(bufferId) == this->buffers.end()) {
            this->buffers[bufferId] = make_shared<ClientBuffer>(bufferId);
            this->buffers[bufferId]->enqueuePages(pages);
        }
        else {
            this->buffers[bufferId]->enqueuePages(pages);
        }
    }

    void enqueueToProcess(vector<shared_ptr<DataPage>> pages) {
        for(int i = 0 ; i < pages.size() ; i++) {
            if(pages[i]->isEndPage())
            {
                if(this->endPageFound == false) {
                    this->endPageFound = true;
                }
            }
            this->unProcessedPages->Put(pages[i]);
        }
    }


};



#endif //OLVP_SHUFFLECACHE_HPP