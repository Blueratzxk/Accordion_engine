//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_OUTPUTBUFFER_HPP
#define OLVP_OUTPUTBUFFER_HPP

//#include "../../Page/DataPage.hpp"
//#include "OutputBufferSchema.hpp"
#include <string>
#include <vector>
using namespace std;
class TaskContext;
class DataPage;
class OutputBufferSchema;
class OutputBuffer
{
public:
    virtual string getInfo() = 0;
    virtual string getOutputBufferName() = 0;
    virtual vector<std::shared_ptr<DataPage>> getPages(string bufferId,long token) = 0;
    virtual void enqueue(vector<std::shared_ptr<DataPage>> pages) = 0;
    virtual bool isFull() = 0;
    virtual void setOutputBuffersSchema(OutputBufferSchema schema) = 0;
    virtual vector<std::shared_ptr<DataPage>> getPages(string bufferId,long token,int dataSize) = 0;
    virtual  void destoryPages() = 0;
    virtual void closeBuffer(string bufferId) = 0;
    virtual void closeBufferGroup(string groupId){};
    virtual bool isEmpty() = 0;
    virtual void changeBufferSize(){};
    virtual int getFillSize(){return 0;}
    virtual void addTaskContext(shared_ptr<TaskContext> taskContext){}
    virtual void triggerNoteEvent(string taskId,string bufferId,string note){}
    virtual void regOutputOperator() = 0;
};


#endif //OLVP_OUTPUTBUFFER_HPP
