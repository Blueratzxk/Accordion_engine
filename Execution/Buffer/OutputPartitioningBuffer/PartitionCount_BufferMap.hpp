//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_PARTITIONCOUNT_BUFFERMAP_HPP
#define OLVP_PARTITIONCOUNT_BUFFERMAP_HPP


//#include "../ClientBuffer.hpp"
//#include "../../../Page/DataPageBuilder.hpp"
#include "BufferShuffler/HashShuffler.hpp"

#include "PartitionResultCache.hpp"
//#include "../../../Utils/SafeMap.hpp"
class PartitionCount_BuffersMap:enable_shared_from_this<PartitionCount_BuffersMap>
{
    int startPartitionNumber = 0;
    int partitionCount = 0;

    atomic<int> pagesHad = 0;
    tbb::concurrent_queue<shared_ptr<DataPage>> unProcessedPagesForRepeat;

    shared_ptr<tbb::concurrent_map<string,shared_ptr<ClientBuffer>>> buffers;
    shared_ptr<mutex> buffersLock;

    shared_ptr<HashShuffler> shuffler;
    mutex groupLock;

    bool endPageFound = false;
    atomic<int> activeShuffleExecutorNumber = 0;

    shared_ptr<DataPage> endPageAddr;

    bool receivedGlobalPages = false;

    atomic<bool> groupStatus = false;

    bool enqueueEndFound = false;

    vector<int> hashColumns ;

    shared_ptr<vector<atomic<int>>> buffersRecord;
    shared_ptr<BlockQueue<shared_ptr<DataPage>>> unProcessedPages = make_shared<BlockQueue<shared_ptr<DataPage>>>();


    pCache partitionResultCache = NULL;
    int cacheReceivedCounter = 0;
    mutex cacheLock;

    bool repeatable = false;

    int pageNumLimit = 10;

    int shuffleExecutorNums = 0;

    int downStreamBuildCount = 0;



    class ShuffleExecutor
    {
        PartitionCount_BuffersMap * pb;

    public:
        ShuffleExecutor(PartitionCount_BuffersMap *pb){
            this->pb = pb;
        }

        PartitionCount_BuffersMap * getpb()
        {
            return this->pb;
        }


        static void releaseShuffleExecutor(shared_ptr<ShuffleExecutor> shuffleExecutor) {
            shuffleExecutor->getpb()->executorTidReportNum++;
            thread th(shuffler,shuffleExecutor->getpb());
            th.detach();
        }
        static void shuffler(PartitionCount_BuffersMap * pb){

            pb->shuffleExecutorNums++;
            __pid_t id = gettid();

            pb->tidSetShufflerLock.lock();
            pb->shuffleExecutorsTids.insert(id);
            pb->tidSetShufflerLock.unlock();
            pb->executorTidReportNum--;

            while(true)
            {
                wait:

                shared_ptr<DataPage> pageGet = pb->unProcessedPages->Take();

                if(pageGet->isShuffleExecutorExitPage()) {
                    pb->shuffleExecutorNums--;
                    pb->taskContext->removeTids({id});
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
                        (*pb->buffers)[to_string(pb->startPartitionNumber+j)]->enqueuePages({pb->endPageAddr});
                        spdlog::debug(to_string(long(pb))+to_string(pb->startPartitionNumber+j) + " add end page!");

                        (*pb->buffersRecord)[pb->startPartitionNumber+j]++;



                        //--------------------cache partitions-------------------------//
                        if((*pb).partitionResultCache != NULL) {
                            (*pb).cacheLock.lock();
                            (*(*pb).partitionResultCache)[j].push_back(pb->endPageAddr);

                            if((*pb).repeatable && (*pb).partitionResultCache != NULL) {

                                for(auto bufferIds : (*pb).delegateBuffers) {

                                    (*(*pb).buffers)[to_string(bufferIds[j])]->enqueuePages({pb->endPageAddr});

                                }
                            }

                            (*pb).cacheLock.unlock();
                        }




                        //-----------------------cache partitions---------------------//

                    }

                    pb->groupStatus = false;
                  //  return;


                }
                else {

                    HashShuffler localShuffler(pb->hashColumns);
                    vector<map<int, shared_ptr<DataPage>>> ShuffledPages;
                    localShuffler.processOptimize(pb->partitionCount, pageGet);
                    ShuffledPages.push_back(localShuffler.buildMap());
                    for (auto pageMap: ShuffledPages) {
                        for (auto page: pageMap) {
                            (*pb->buffers)[to_string(pb->startPartitionNumber + page.first)]->enqueuePages({page.second});
                            (*(pb->buffersRecord))[pb->startPartitionNumber + page.first]++;



                            //--------------------cache partitions-------------------------//


                            if((*pb).partitionResultCache != NULL) {
                                (*pb).cacheLock.lock();
                                (*(*pb).partitionResultCache)[page.first].push_back(page.second);
                                if((*pb).repeatable && (*pb).partitionResultCache != NULL) {
                                    for(auto bufferIds : (*pb).delegateBuffers) {
                                        (*(*pb).buffers)[to_string(bufferIds[page.first])]->enqueuePages({page.second});
                                    }
                                }
                                (*pb).cacheLock.unlock();






                            }
                          //-----------------------cache partitions---------------------//



                        }
                    }
                }

                pb->activeShuffleExecutorNumber--;
            }
        }

    };

    shared_ptr<ShuffleExecutor> shuffleExecutors;



    vector<vector<int>> delegateBuffers;
    map<int,bool> delegatePutReceivedPages;

    shared_ptr<TaskContext> taskContext;
    atomic<int> executorTidReportNum = 0;


    mutex tidsLock;
    mutex tidSetShufflerLock;
    set<__pid_t> shuffleExecutorsTids;


    int taskGroupTraffic;




public:


    PartitionCount_BuffersMap(shared_ptr<TaskContext> taskContext,int forceExecutorNum,int startPartitionNumber,int partitionCount,shared_ptr<tbb::concurrent_map<string,shared_ptr<ClientBuffer>>> buffersIn,shared_ptr<mutex> buffersLock,vector<int> hashColumns,bool repeatable)
    {
        this->startPartitionNumber = startPartitionNumber;
        this->partitionCount = partitionCount;
        this->buffers = buffersIn;
        this->shuffler = make_shared<HashShuffler>(hashColumns);
        this->groupStatus = true;
        this->buffersLock = buffersLock;
        this->hashColumns = hashColumns;

        this->repeatable = repeatable;

        this->buffersRecord = make_shared<vector<atomic<int>>>(50);

        this->shuffleExecutors = make_shared<ShuffleExecutor>(this);

        this->taskContext = taskContext;

        if(forceExecutorNum > 0)
        {
            tidsLock.lock();
            for(int i = 0 ; i < forceExecutorNum ; i++)
                ShuffleExecutor::releaseShuffleExecutor(this->shuffleExecutors);

            while(this->executorTidReportNum > 0);

            if(this->taskContext != NULL)
                this->taskContext->addTids(this->shuffleExecutorsTids);

            tidsLock.unlock();
        }
        else {
            tidsLock.lock();
            for (int i = 0; i < this->partitionCount; i++)
                ShuffleExecutor::releaseShuffleExecutor(this->shuffleExecutors);

            while(this->executorTidReportNum > 0);

            if(this->taskContext != NULL)
                this->taskContext->addTids(this->shuffleExecutorsTids);
            tidsLock.unlock();
        }
    }

    PartitionCount_BuffersMap(shared_ptr<TaskContext> taskContext,int forceExecutorNum,int startPartitionNumber,int partitionCount,shared_ptr<tbb::concurrent_map<string,shared_ptr<ClientBuffer>>> buffersIn,
                              shared_ptr<mutex> buffersLock,vector<int> hashColumns,bool repeatable,pCache prc)
    {
        this->startPartitionNumber = startPartitionNumber;
        this->partitionCount = partitionCount;
        this->buffers = buffersIn;
        this->shuffler = make_shared<HashShuffler>(hashColumns);
        this->groupStatus = true;
        this->buffersLock = buffersLock;
        this->hashColumns = hashColumns;

        this->buffersRecord = make_shared<vector<atomic<int>>>(50);

        this->shuffleExecutors = make_shared<ShuffleExecutor>(this);

        this->partitionResultCache = prc;

        this->repeatable = repeatable;

        if(taskContext != NULL)
            this->taskContext = taskContext;
        else
            this->taskContext = NULL;

        //if it is buildBuffer,and need to cache,we process these pages.Else,it just to use processed pages.
        if(this->repeatable && this->partitionResultCache != NULL) {

            if(forceExecutorNum > 0)
            {
                for(int i = 0 ; i < forceExecutorNum ; i++)
                    ShuffleExecutor::releaseShuffleExecutor(this->shuffleExecutors);
            }
            else
                for(int i = 0 ; i < this->partitionCount ; i++)
                    ShuffleExecutor::releaseShuffleExecutor(this->shuffleExecutors);
        }
        else if(this->repeatable && this->partitionResultCache == NULL)
        {
        }
        else
        {
            if(forceExecutorNum > 0)
            {
                tidsLock.lock();
                for(int i = 0 ; i < forceExecutorNum ; i++)
                    ShuffleExecutor::releaseShuffleExecutor(this->shuffleExecutors);

                while(this->executorTidReportNum > 0);

                if(this->taskContext != NULL)
                    this->taskContext->addTids(this->shuffleExecutorsTids);

                tidsLock.unlock();
            }
            else {
                tidsLock.lock();
                for (int i = 0; i < this->partitionCount; i++)
                    ShuffleExecutor::releaseShuffleExecutor(this->shuffleExecutors);

                while(this->executorTidReportNum > 0);

                if(this->taskContext != NULL)
                    this->taskContext->addTids(this->shuffleExecutorsTids);
                tidsLock.unlock();
            }
        }
    }

    void addTaskGroupTraffic(int num)
    {
        this->taskGroupTraffic+=num;
    }
    void resetTaskGroupTraffic()
    {
        this->taskGroupTraffic = 0;
    }
    int getTaskGroupTraffic()
    {
        return this->taskGroupTraffic;
    }

    void addTaskContext(shared_ptr<TaskContext> taskContext)
    {
        this->tidsLock.lock();
        if(this->taskContext == NULL) {
            this->taskContext = taskContext;
            if(!this->shuffleExecutorsTids.empty())
                this->taskContext->addTids(this->shuffleExecutorsTids);
        }

        this->tidsLock.unlock();
    }


    void addDelegateBuffer(vector<int> bufferIds)
    {
        this->cacheLock.lock();

        this->delegateBuffers.push_back(bufferIds);


        if(this->repeatable && this->partitionResultCache != NULL) {
            for (int i = 0; i < bufferIds.size(); i++) {
                (*this->buffers)[to_string(bufferIds[i])]->enqueuePages((*this->partitionResultCache)[i]);
            }
        }

        this->cacheLock.unlock();
    }

    bool recordBuildCount()
    {
        this->downStreamBuildCount++;
        if(this->downStreamBuildCount == this->partitionCount)
            return true;
        else
            return false;
    }

    bool isReceivedGlobalPages()
    {
        return this->receivedGlobalPages;
    }
    void receiveGlobalPages()
    {
        this->receivedGlobalPages = true;
    }
    int getPartitionCount()
    {
        return this->partitionCount;
    }


    bool haveEndPage()
    {
        return endPageFound;
    }
    bool isBufferInGroup(int bufferId)
    {
        if(bufferId >= startPartitionNumber && bufferId < startPartitionNumber + partitionCount)
            return true;
        else
            return false;
    }
    int getBlockQueueSize()
    {
        return this->unProcessedPages->Size();
    }

    int getGroupStorePagesSize() {

        //   int size = this->unProcessedPagesForRepeat.unsafe_size();
        this->buffersLock->lock();
        if(this->buffers == NULL) {
            this->buffersLock->unlock();
            return INT_MAX;
        }

        int size = 0;
        for (auto buffer: *this->buffers) {
            int bufferNumPage = buffer.second->getPageNums();
            if(bufferNumPage > size)
                size = bufferNumPage;
        }

        this->buffersLock->unlock();

        return size;

    }
    void enqueuePages(vector<shared_ptr<DataPage>> pages)
    {

        for(int i = 0 ; i < pages.size() ; i++) {
            if(pages[i]->isEndPage())
            {
                if(this->enqueueEndFound == false) {
                    this->groupStatus = false;
                    this->enqueueEndFound = true;
                }
                else {
                    cout << "WTF!" << endl;
                    return;
                }
            }
            this->unProcessedPagesForRepeat.push(pages[i]);
            this->unProcessedPages->Put(pages[i]);
            pagesHad++;
        }


    }


    vector<int> getGroupBufferIds()
    {
        vector<int> ids;
        for(int i = 0 ; i < partitionCount ; i++)
        {
            ids.push_back(startPartitionNumber + i);
        }
        return ids;
    }



    void closeGroupTasks(shared_ptr<DataPage> page)
    {
        this->groupLock.lock();
        if(this->groupStatus == true) {
            this->unProcessedPagesForRepeat.push(page);
            spdlog::info("Closing Task Group!"+ to_string(this->startPartitionNumber)+"|"+ to_string(this->partitionCount)+"|"+
                                                                                                                           to_string(long(this))+"!");
            this->unProcessedPages->Put(page);
            this->groupStatus = false;
        }
        this->groupLock.unlock();

    }
    bool getGroupStatus()
    {
        this->groupLock.lock();
        bool status = this->groupStatus;
        this->groupLock.unlock();
        return status;
    }



    int getStartPartitionNumber(){
        return this->startPartitionNumber;
    }




};



#endif //OLVP_PARTITIONCOUNT_BUFFERMAP_HPP
