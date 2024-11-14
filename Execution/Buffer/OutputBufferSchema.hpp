//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_OUTPUTBUFFERSCHEMA_HPP
#define OLVP_OUTPUTBUFFERSCHEMA_HPP


#include "nlohmann/json.hpp"
#include "../../Partitioning/PartitioningScheme.hpp"
using namespace std;


class OutputBufferId
{
    string id;
public:
    OutputBufferId(string id)
    {
        this->id = id;
    }
    string get()
    {
        return this->id;
    }

};



class OutputBufferSchema
{
public:
    enum BufferType
    {
        NONE,
        PARTITIONED,
        BROADCAST,
        ARBITRARY,
        DISCARDING,
        SPOOLING,
        SIMPLE,
        SIMPLE_SHUFFLE,
        PARTITIONED_HASH,
        SHUFFLE_STAGE
    };
    enum PartitioningBufferType
    {
        PAR_NONE,
        PAR_REPEATABLE,
        PAR_ONCE
    };
private:
    BufferType type;
    map<string, int> buffers;
    shared_ptr<PartitioningScheme> partitioning_Scheme;
    PartitioningBufferType parBufType = PAR_NONE;
    map<int,int> taskGroupMap;


public:
    OutputBufferSchema(){}


    void setOutputPartitioningScheme(shared_ptr<PartitioningScheme> pScheme,PartitioningBufferType pType)
    {
        this->parBufType = pType;
        this->partitioning_Scheme = pScheme;
    }
    shared_ptr<PartitioningScheme> getParScheme()
    {
        return this->partitioning_Scheme;
    }
    bool isPartitioning_Buffer()
    {
        if(this->parBufType == this->PAR_NONE)
            return false;
        return true;
    }
    PartitioningBufferType getParBufType()
    {
        return this->parBufType;
    }

    void updateTaskGroupMap(map<int,int> tgm)
    {
        this->taskGroupMap = tgm;
    }
    map<int,int> getTaskGroupMap()
    {
        return this->taskGroupMap;
    }


    static shared_ptr<OutputBufferSchema> createInitialEmptyOutputBufferSchema(BufferType type)
    {
        map<string,int> initial;
        //initial["0"] = 0;
        return make_shared<OutputBufferSchema>(type,initial);
    }
    static shared_ptr<OutputBufferSchema> createEmptyOutputBufferSchemaWithPartitioningScheme(BufferType type,shared_ptr<PartitioningScheme> scheme,PartitioningBufferType pType)
    {
        map<string,int> initial;
        //initial["0"] = 0;
        shared_ptr<OutputBufferSchema> schema = make_shared<OutputBufferSchema>(type,initial);
        schema->setOutputPartitioningScheme(scheme,pType);
        return  schema;
    }
    static OutputBufferSchema createEmptyOutputBufferSchema()
    {
        map<string,int> initial;
        return  OutputBufferSchema(BufferType::NONE,initial);
    }

    static OutputBufferSchema createEmptyOutputBufferSchema(PartitioningHandle handle)
    {
        BufferType type;
        if(handle.equals(*SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION")))
            type = BROADCAST;
        else if(handle.equals(*SystemPartitioningHandle::get("ARBITRARY_DISTRIBUTION")))
            type = ARBITRARY;
        else
            type = PARTITIONED;
        map<string,int> initial;
        return  OutputBufferSchema(type,initial);
    }

    bool isEmptyOutputBuffer()
    {
        return this->type == BufferType::NONE?true:false;
    }

    OutputBufferSchema(BufferType type,map<string,int> buffers){
        this->type = type;
        this->buffers = buffers;
    }
    OutputBufferSchema(BufferType type,map<string,int> buffers,shared_ptr<PartitioningScheme> scheme,PartitioningBufferType pType){
        this->type = type;
        this->buffers = buffers;
        this->partitioning_Scheme = scheme;
        this->parBufType = pType;
    }
    map<string, int>& getBuffers()
    {
        return this->buffers;
    }
    void changeBuffers(map<string, int> bs)
    {
        this->buffers = bs;
    }
    BufferType &getBufferType()
    {
        return this->type;
    }
    static string Serialize(OutputBufferSchema schema)
    {
        nlohmann::json outputBuffersSchema;


        if(!schema.isPartitioning_Buffer())
        {
            outputBuffersSchema["parBufType"] = schema.getParBufType();
        }
        else {
            outputBuffersSchema["parBufType"] = schema.getParBufType();
            outputBuffersSchema["PartitioningScheme"] = schema.getParScheme()->Serialize();

        }

        outputBuffersSchema["type"] = schema.getBufferType();
        outputBuffersSchema["taskGroupMap"] = schema.getTaskGroupMap();
        outputBuffersSchema["buffers"] = schema.getBuffers();
        nlohmann::json json;
        json["outputBuffersSchema"] = outputBuffersSchema;
        return json.dump();
    }
    static shared_ptr<OutputBufferSchema> Deserialize(string schema)
    {
        if(schema == "NULL")
            return NULL;

        nlohmann::json json = nlohmann::json::parse(schema);



        nlohmann::json outputBuffersSchemaJson = json["outputBuffersSchema"];
        BufferType type = outputBuffersSchemaJson["type"];

        // BufferType type = mapping[typeNum];
        map<string,int> buffers = outputBuffersSchemaJson["buffers"];
        shared_ptr<OutputBufferSchema> buffer = make_shared<OutputBufferSchema>(type, buffers);

        map<int,int> taskGroupMap = outputBuffersSchemaJson["taskGroupMap"];


        PartitioningBufferType pFlag = outputBuffersSchemaJson["parBufType"];
        PartitioningScheme ps;
        if(pFlag != PAR_NONE)
        {
            buffer->setOutputPartitioningScheme(ps.Deserialize(outputBuffersSchemaJson["PartitioningScheme"]),pFlag);
        }

        buffer->updateTaskGroupMap(taskGroupMap);

        return buffer;
    }




};




#endif //OLVP_OUTPUTBUFFERSCHEMA_HPP
