//
// Created by zxk on 9/29/23.
//

#ifndef OLVP_SHUFFLESTAGEBUFFER_HPP
#define OLVP_SHUFFLESTAGEBUFFER_HPP




#include "../OutputBuffer.hpp"

#include "../OutputBufferSchema.hpp"
//#include "BufferShuffler/SimplePageHashGenerator.hpp"
#include <atomic>
#include "PartitionCount_BufferMap.hpp"
#include "TaskBufferGroup.hpp"

class ShuffleStageBuffer: public OutputBuffer
{


    atomic<int> pageNumsLimit = 10;
    atomic<int> maxPageNumsLimit = 1000;
    shared_ptr<OutputBufferSchema> bufferSchema = OutputBufferSchema::createInitialEmptyOutputBufferSchema(OutputBufferSchema::BufferType::PARTITIONED_HASH);
    //map<string,clientBuffer*> buffers;



    list<PartitionCount_BuffersMap> partitionCount_taskGroupMap;


    int regBufferSize = 0;

    mutex buffersLock;
    mutex tbgLock;
    atomic<bool> endPageFound = false;
    shared_ptr<DataPage> endPageAddr;

    vector<shared_ptr<DataPage>> allPages;

    shared_ptr<TaskBufferGroup> tbg;

    int sizeForChange = 2;

    shared_ptr<TaskContext> taskContext = NULL;

    atomic<long> remainingTuples = 0;

    atomic<int> outputOperatorCount = 0;
    atomic<int> endPageNum = 0;

public:


    ShuffleStageBuffer(OutputBufferSchema schema){
        tbg = make_shared<TaskBufferGroup>();

        if(!schema.isPartitioning_Buffer())
        {
            cout << "ERROR! hashShuffleOutputBuffer must be partitioningOutputBuffer!"<<endl;
            exit(0);
        }

        vector<string> hashColumns = schema.getParScheme()->getHashColumns();
        vector<int> hashChannelIndexs ;
        for(auto col : hashColumns)
        {
            hashChannelIndexs.push_back(atoi(col.c_str()));
        }
        tbg->setBuffers(hashChannelIndexs,schema.getParBufType());

        this->setOutputBuffersSchema(schema);

        if(schema.getParBufType() == OutputBufferSchema::PAR_REPEATABLE)
            this->pageNumsLimit = 1000;
        else {
            this->pageNumsLimit = 1;
            this->sizeForChange = 1;
        }


        //  this->addPartitionTaskGroup(this->getInitialPartitionCount());

    }

    void addTaskContext(shared_ptr<TaskContext> taskContext){
        this->taskContext = taskContext;
    }

    string getInfo() {
        return "ShuffleStageBuffer";
    }

    string getOutputBufferName()
    {
        return "ShuffleStageBuffer";
    }

    void addPartitionTaskGroup(int newPartitionCount)
    {
        if(newPartitionCount > 0) {
            this->tbg->addPartitionTaskGroup(newPartitionCount);


        }
    }

    void closePartitionTaskGroup(int groupId)
    {
        this->tbg->closeGroupInOnceType(groupId);
    }

    void regOutputOperator(){
        this->outputOperatorCount++;
    }



    vector<shared_ptr<DataPage>> getPages(string bufferId,long token) {

        vector<shared_ptr<DataPage>> pages = this->tbg->getPages(bufferId,token,5);

        if (!this->bufferSchema->getParBufType() == OutputBufferSchema::PAR_REPEATABLE)
            for(auto page:pages) {
                this->remainingTuples -= page->getElementsCount();

                if (this->taskContext != NULL)
                    this->taskContext->updateRemainingBufferTupleCount(-page->getElementsCount());

            }

        return pages;
    }

    vector<shared_ptr<DataPage>> getPages(string bufferId,long token,int pageNums) {

        vector<shared_ptr<DataPage>> pages = this->tbg->getPages(bufferId,token,pageNums);

        if (!this->bufferSchema->getParBufType() == OutputBufferSchema::PAR_REPEATABLE)
            for(auto page:pages) {
                this->remainingTuples -= page->getElementsCount();

                if (this->taskContext != NULL)
                    this->taskContext->updateRemainingBufferTupleCount(-page->getElementsCount());
            }

        return pages;
    }


    void enqueue(vector<shared_ptr<DataPage>> pages) {


        vector<shared_ptr<DataPage>> pagesToEnqueue;
        for (int i = 0; i < pages.size(); i++) {
            if (pages[i]->isEndPage()) {

                this->endPageNum++;
                if(this->endPageNum == this->outputOperatorCount) {
                    this->endPageFound = true;
                    endPageAddr = pages[i];
                    pagesToEnqueue.push_back(pages[i]);
                }
            }
            else
                pagesToEnqueue.push_back(pages[i]);
        }

        bool isRepeatable = false;

        //Repeatable means this buffer is used to redistribution.And redistribution may happen when all pages haven't loaded to buffer.
        //Redistribution does not always occur after the data is fully loaded to buffer.
        //So we have to do loading and redistribution at the same time.
        //So we must check that if a new task buffer group has already loaded the existing pages in global buffer.
        //if so,then we just store pages to all-pages-structure and wait for the subsequent distribution.
        if (this->bufferSchema->getParBufType() == OutputBufferSchema::PAR_REPEATABLE) {
            this->tbg->balanceGlobalPages();
            this->tbg->enqueueGlobalPages(pagesToEnqueue);
            isRepeatable = true;
        }


        //Determine how to distribute pages based on the buffer operating mode.
        //if repeatable,all group get same pages.
        //if not,just select one group.
        vector<shared_ptr<PartitionCount_BuffersMap>> pB = this->tbg->selectGroups(isRepeatable);




        if (isRepeatable) {
            for (int i = 0; i < pB.size(); i++) {
                pB[i]->enqueuePages(pagesToEnqueue);
            }
        }
        else
        {
            pB[0]->enqueuePages(pagesToEnqueue);

            for(auto page : pagesToEnqueue) {
                this->remainingTuples += (page->getElementsCount());
                if(this->taskContext != NULL)
                    this->taskContext->updateRemainingBufferTupleCount(page->getElementsCount());
            }
            if(this->taskContext != NULL)
                this->taskContext->setLastEnqueuedTupleCount(this->remainingTuples);

            if(this->endPageFound)
            {
                this->tbg->setEndPageSignal(this->endPageAddr);
                this->tbg->addEndPageForUnselectedGroup(this->endPageAddr);
            }
        }

        this->tbg->tuneBufferCapacity("producer");



    }
    bool isFull() {

        if(this->bufferSchema->getParBufType() == this->bufferSchema->PAR_REPEATABLE)
            return false;
        else
            return this->tbg->isFull();


    }

    void changeBufferSize()
    {
        this->pageNumsLimit += this->sizeForChange;
        if(this->pageNumsLimit <=0)
            this->pageNumsLimit = 1;
        if(this->pageNumsLimit > this->maxPageNumsLimit)
            this->pageNumsLimit = maxPageNumsLimit.load();
        //  this->pageNumsLimit = 10;
        spdlog::debug("Shuffle Buffer size is changed! Now the size is "+ to_string(this->pageNumsLimit)+"!");
    }
    void triggerNoteEvent(string taskId,string bufferId,string note) {
        spdlog::debug(taskId+" 's OutputPartitioningBuffer BufferId "+bufferId+" NoteEvent Detected! Note is "+note);
        tbg->reportDownStreamTaskBuildCompletedInfo(bufferId);
    }

    bool isEmpty()
    {
        return tbg->isEmpty();
    }
    void destoryPages()
    {

    }

    void closeBuffer(string bufferId)
    {

    }

    void checkTaskGroup(map<int,int> parentStageTaskGroup)
    {
        map<int,int> interTg = tbg->getTaskGroupMap();

        list<int> tgNeedToBuild;

        if(interTg.size() <= parentStageTaskGroup.size()) {
            for (int i = 0; i < interTg.size(); i++) {
                if(parentStageTaskGroup[i] != interTg[i])
                {
                    spdlog::critical("TaskGroupMap inconsistent!");
                }
            }
        }
        else
        {
            spdlog::critical("TaskGroupMap inconsistent!");

        }

        if(interTg.size() < parentStageTaskGroup.size())
        {
            int gap = parentStageTaskGroup.size() - interTg.size();
            for(int i = 0 ; i < gap ; i++)
            {
                tgNeedToBuild.push_front(parentStageTaskGroup[parentStageTaskGroup.size() -1 - i]);
            }
        }

        for(auto pn : tgNeedToBuild)
        {
            this->addPartitionTaskGroup(pn);
        }

    }

    void setOutputBuffersSchema(OutputBufferSchema schema){

        tbgLock.lock();

        this->bufferSchema = make_shared<OutputBufferSchema>(schema.getBufferType(),schema.getBuffers(),schema.getParScheme(),schema.getParBufType());
        this->bufferSchema->updateTaskGroupMap(schema.getTaskGroupMap());
        map<string,int> tBuffers = this->bufferSchema->getBuffers();


        int newBufferSize = tBuffers.size();

        if(this->regBufferSize == 0)
        {
            map<int, int> parentStageTaskGroupMap = this->bufferSchema->getTaskGroupMap();

            for(auto tgm : parentStageTaskGroupMap)
            {
                this->addPartitionTaskGroup(tgm.second);
            }
        }
        else {
            map<int, int> parentStageTaskGroupMap = this->bufferSchema->getTaskGroupMap();
            this->checkTaskGroup(parentStageTaskGroupMap);
        }

        this->regBufferSize = newBufferSize;

        tbgLock.unlock();
    }
};



#endif //OLVP_SHUFFLESTAGEBUFFER_HPP
