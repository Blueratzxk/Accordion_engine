//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SIMPLEOUTPUTBUFFERMANAGER_HPP
#define OLVP_SIMPLEOUTPUTBUFFERMANAGER_HPP

#include "OutputBufferManager.hpp"
#include "OutputBufferSchema.hpp"

class SimpleOutputBufferManager:public OutputBufferManager
{
public:
    shared_ptr<OutputBufferSchema> schema = OutputBufferSchema::createInitialEmptyOutputBufferSchema(OutputBufferSchema::BufferType::SIMPLE);

    SimpleOutputBufferManager(): OutputBufferManager("SimpleOutputBufferManager"){

    }
    shared_ptr<OutputBufferSchema> getSchema()
    {
        return this->schema;
    }
    vector<shared_ptr<OutputBufferSchema>> addOutputBuffers(vector<OutputBufferId> newBuffers)
    {
        bool isChanged = false;
        map<string,int> buffers = schema->getBuffers();

        for(int i = 0 ; i < newBuffers.size() ; i++)
        {
            if(buffers.find(newBuffers[i].get()) == buffers.end())
            {
                isChanged = true;
                buffers[newBuffers[i].get()] = 0;
            }
        }
        if(isChanged)
        {
            this->schema->changeBuffers(buffers);
            return {schema};
        }
        else
        {
            return {};
        }

    }

};


#endif //OLVP_SIMPLEOUTPUTBUFFERMANAGER_HPP
