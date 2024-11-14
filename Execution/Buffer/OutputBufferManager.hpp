//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_OUTPUTBUFFERMANAGER_HPP
#define OLVP_OUTPUTBUFFERMANAGER_HPP

//#include "OutputBufferSchema.hpp"

class OutputBufferManager{
    string managerId;
public:
    OutputBufferManager(string managerId)
    {
        this->managerId = managerId;
    }
    virtual string getOutputBufferManagerId(){
        return this->managerId;
    }
    virtual vector<shared_ptr<OutputBufferSchema>> addOutputBuffers(vector<OutputBufferId> newBuffers) = 0;
    virtual shared_ptr<OutputBufferSchema> getSchema(){ return NULL;};
};



#endif //OLVP_OUTPUTBUFFERMANAGER_HPP
