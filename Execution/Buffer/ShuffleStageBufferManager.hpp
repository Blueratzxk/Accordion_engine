//
// Created by zxk on 9/29/23.
//

#ifndef OLVP_SHUFFLESTAGEBUFFERMANAGER_HPP
#define OLVP_SHUFFLESTAGEBUFFERMANAGER_HPP


#include "OutputBufferManager.hpp"
#include "OutputBufferSchema.hpp"
#include "../../Partitioning/PartitioningScheme.hpp"

class ShuffleStageBufferManager:public OutputBufferManager
{
public:
    shared_ptr<OutputBufferSchema> schema;
    ShuffleStageBufferManager(shared_ptr<PartitioningScheme> scheme): OutputBufferManager("ShuffleStageBufferManager"){
        schema = OutputBufferSchema::createEmptyOutputBufferSchemaWithPartitioningScheme(OutputBufferSchema::SHUFFLE_STAGE,scheme,OutputBufferSchema::PAR_ONCE);
    }

    ShuffleStageBufferManager(shared_ptr<PartitioningScheme> scheme,OutputBufferSchema::PartitioningBufferType pType): OutputBufferManager("ShuffleStageBufferManager"){
        schema = OutputBufferSchema::createEmptyOutputBufferSchemaWithPartitioningScheme(OutputBufferSchema::SHUFFLE_STAGE,scheme,pType);
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


#endif //OLVP_SHUFFLESTAGEBUFFERMANAGER_HPP
