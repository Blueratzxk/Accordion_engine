//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_OUTPUTPARTITIONINGBUFFERMANAGER_HPP
#define OLVP_OUTPUTPARTITIONINGBUFFERMANAGER_HPP


#include "OutputBufferManager.hpp"
#include "OutputBufferSchema.hpp"
//#include "../../Partitioning/PartitioningScheme.hpp"

class OutputPartitioningBufferManager:public OutputBufferManager
{
public:
    shared_ptr<OutputBufferSchema> schema;
    OutputPartitioningBufferManager(shared_ptr<PartitioningScheme> scheme): OutputBufferManager("OutputPartitioningBufferManager"){
        schema = OutputBufferSchema::createEmptyOutputBufferSchemaWithPartitioningScheme(OutputBufferSchema::PARTITIONED_HASH,scheme,OutputBufferSchema::PAR_ONCE);
    }

    OutputPartitioningBufferManager(shared_ptr<PartitioningScheme> scheme,OutputBufferSchema::PartitioningBufferType pType): OutputBufferManager("OutputPartitioningBufferManager"){
        schema = OutputBufferSchema::createEmptyOutputBufferSchemaWithPartitioningScheme(OutputBufferSchema::PARTITIONED_HASH,scheme,pType);
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

#endif //OLVP_OUTPUTPARTITIONINGBUFFERMANAGER_HPP
