//
// Created by zxk on 7/9/24.
//

#ifndef OLVP_BUFFERINFODESCRIPTOR_HPP
#define OLVP_BUFFERINFODESCRIPTOR_HPP

#include "nlohmann/json.hpp"
#include <string>
using namespace std;
class BufferInfoDescriptor
{

    int taskOutputBufferSizeTurnDownCounter;
    int taskOutputBufferSizeTurnUpCounter;
    int exchangeBufferSizeTurnUpCounter;
    int exchangeBufferSizeTurnDownCounter;

public:
    BufferInfoDescriptor()
    {
        this->exchangeBufferSizeTurnDownCounter = 0;
        this->exchangeBufferSizeTurnUpCounter = 0;
        this->taskOutputBufferSizeTurnDownCounter = 0;
        this->taskOutputBufferSizeTurnUpCounter = 0;
    }
    BufferInfoDescriptor(  int taskOutputBufferSizeTurnDownCounter,int taskOutputBufferSizeTurnUpCounter,
                           int exchangeBufferSizeTurnUpCounter,int exchangeBufferSizeTurnDownCounter)
    {
        this->exchangeBufferSizeTurnDownCounter = exchangeBufferSizeTurnDownCounter;
        this->exchangeBufferSizeTurnUpCounter = exchangeBufferSizeTurnUpCounter;
        this->taskOutputBufferSizeTurnDownCounter = taskOutputBufferSizeTurnDownCounter;
        this->taskOutputBufferSizeTurnUpCounter = taskOutputBufferSizeTurnUpCounter;
    }
    int getTb_TDown(){return this->taskOutputBufferSizeTurnDownCounter;}
    int getTb_TUp(){return this->taskOutputBufferSizeTurnUpCounter;}
    int getEb_TUp(){return this->exchangeBufferSizeTurnUpCounter;}
    int getEb_TDown(){return this->exchangeBufferSizeTurnDownCounter;}

    static string Serialize(BufferInfoDescriptor bufferInfoDescriptor)
    {
        nlohmann::json json;

        json["taskOutputBufferSizeTurnDownCounter"] = bufferInfoDescriptor.taskOutputBufferSizeTurnDownCounter;
        json["taskOutputBufferSizeTurnUpCounter"] = bufferInfoDescriptor.taskOutputBufferSizeTurnUpCounter;
        json["exchangeBufferSizeTurnUpCounter"] = bufferInfoDescriptor.exchangeBufferSizeTurnUpCounter;
        json["exchangeBufferSizeTurnDownCounter"] = bufferInfoDescriptor.exchangeBufferSizeTurnDownCounter;


        string result = json.dump();
        return result;
    }

    static shared_ptr<BufferInfoDescriptor> Deserialize(string bufferInfoDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(bufferInfoDescriptor);

        auto result = make_shared<BufferInfoDescriptor>(json["taskOutputBufferSizeTurnDownCounter"],
                                                      json["taskOutputBufferSizeTurnUpCounter"],
                                                      json["exchangeBufferSizeTurnUpCounter"],
                                                      json["exchangeBufferSizeTurnDownCounter"]);

        return  result;
    }

};


#endif //OLVP_BUFFERINFODESCRIPTOR_HPP
