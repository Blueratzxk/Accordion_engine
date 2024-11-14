//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_LAZYOUTPUTBUFFER_HPP
#define OLVP_LAZYOUTPUTBUFFER_HPP

#include "OutputBuffer.hpp"
//#include "ClientBuffer.hpp"
#include "OutputBufferSchema.hpp"
#include "BroadcastOutputBuffer.hpp"
#include "SimpleOutputBuffer.hpp"
#include "OutputPartitioningBuffer/OutputPartitioningBuffer.hpp"
#include "OutputPartitioningBuffer/ShuffleStageBuffer.hpp"
#include "SimpleShuffleStageBuffer.hpp"
class LazyOutputBuffer: public OutputBuffer
{

    int pageNumsLimit = 10;

    shared_ptr<TaskId> taskId;
    shared_ptr<OutputBuffer> delegate = NULL;
    atomic<bool> bufferReady = false;
    shared_ptr<TaskContext> taskContext = NULL;
    shared_ptr<SimpleEvent> event;


public:
    LazyOutputBuffer(shared_ptr<TaskId> taskId){
        this->taskId = taskId;
        this->event = make_shared<SimpleEvent>();
    }


    string getInfo() {
        return "lazy buffer";
    }
    string getOutputBufferName()
    {
        return "LazyOutputBuffer";
    }
    bool isBufferReady()
    {
        return this->bufferReady;
    }

    void recordOutputTupleInfos(vector<shared_ptr<DataPage>> *result)
    {
        if(this->taskContext != NULL) {

            if(this->taskContext->getOutputTupleWidth() < 0 && !(*result).empty()&&!(*result)[0]->isEndPage()&&!(*result)[0]->isEmptyPage())
            {
                int tupleByteWidth = 0;
                for(auto type : (*result)[0]->get()->schema()->fields()) {
                    tupleByteWidth += type->type()->byte_width();
                }
                this->taskContext->setOutputTupleWidth(tupleByteWidth);
            }

            int count = 0;
            for (auto r: (*result))
                count += r->getElementsCount();
            this->taskContext->addTotalTupleCount(count);
        }
    }

    void recordInputTupleInfos(vector<shared_ptr<DataPage>> &result)
    {
        if(this->taskContext != NULL) {

            if(this->taskContext->getOutputTupleWidth() < 0 && !(result).empty()&&!(result)[0]->isEndPage()&&!(result)[0]->isEmptyPage())
            {
                int tupleByteWidth = 0;
                for(auto type : (result)[0]->get()->schema()->fields()) {
                    tupleByteWidth += type->type()->byte_width();
                }
                this->taskContext->setOutputTupleWidth(tupleByteWidth);
            }

            int count = 0;
            for (auto r: (result))
                count += r->getElementsCount();
            this->taskContext->addTotalInputTuples(count);
        }
    }

    vector<shared_ptr<DataPage>> getPages(string bufferId,long token) {
        vector<shared_ptr<DataPage>> result;
        if(this->delegate != NULL) {
            this->event->notify();
            result = this->delegate->getPages(bufferId, token);
            recordOutputTupleInfos(&result);
        }

        return result;

    }
    vector<shared_ptr<DataPage>> getPages(string bufferId,long token,int pageNums) {
        vector<shared_ptr<DataPage>> result;
        if(this->delegate != NULL) {
            this->event->notify();
            result = this->delegate->getPages(bufferId, token,pageNums);
            recordOutputTupleInfos(&result);

        }

        return result;

    }

    void enqueue(vector<shared_ptr<DataPage>> pages) {
        while(!bufferReady);
        recordInputTupleInfos(pages);
        this->delegate->enqueue(pages);
    }
    bool isFull() {
        while(!bufferReady);
        if(this->delegate == NULL)
            return false;
        else {

            if(this->delegate->isFull()) {
                this->event->listen();
                return true;
            }
            else
                return false;

        }
    }
    bool isEmpty()
    {
        while(!bufferReady);
        if(this->delegate == NULL)
            return false;
        else
            return this->delegate->isEmpty();
    }
    void triggerNoteEvent(string taskId,string bufferId,string note) {
        if(this->delegate == NULL)
            return;
        else
            return this->delegate->triggerNoteEvent(taskId,bufferId,note);
    }
    void destoryPages()
    {
        this->delegate->destoryPages();
    }
    void closeBuffer(string bufferId)
    {
        if(this->delegate != NULL)
            this->delegate->closeBuffer(bufferId);
    }
    void changeBufferSize()
    {
        if(this->delegate != NULL)
            this->delegate->changeBufferSize();
    }

    void addTaskContext(shared_ptr<TaskContext> taskContext){
        if(this->delegate != NULL)
          this->delegate->addTaskContext(taskContext);

          this->taskContext = taskContext;
    }

    void regOutputOperator(){
        while(!bufferReady);
        this->delegate->regOutputOperator();
    }

    void setOutputBuffersSchema(OutputBufferSchema schema){

        if(this->delegate == NULL) {
            switch (schema.getBufferType()) {

                case OutputBufferSchema::BROADCAST:
                    this->delegate = make_shared<BroadcastOutputBuffer>(schema);
                    break;

                case OutputBufferSchema::SIMPLE:
                    this->delegate = make_shared<SimpleOutputBuffer>(schema);
                    break;

                case OutputBufferSchema::PARTITIONED_HASH:
                    this->delegate = make_shared<OutputPartitioningBuffer>(schema);
                    break;
                case OutputBufferSchema::SHUFFLE_STAGE:
                    this->delegate = make_shared<ShuffleStageBuffer>(schema);
                    break;
                case OutputBufferSchema::SIMPLE_SHUFFLE:
                    this->delegate = make_shared<SimpleShuffleStageBuffer>(schema);
                    break;
                default:
                    this->delegate = make_shared<BroadcastOutputBuffer>(schema);
                    break;
            }
        }


        this->delegate->setOutputBuffersSchema(schema);
        this->bufferReady = true;

    }







};

#endif //OLVP_LAZYOUTPUTBUFFER_HPP
