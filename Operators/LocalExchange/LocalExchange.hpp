//
// Created by zxk on 5/27/23.
//

#ifndef OLVP_LOCALEXCHANGE_HPP
#define OLVP_LOCALEXCHANGE_HPP

#include "../../common.h"
#include "LocalExchanger.hpp"
#include "LocalExchangeSource.hpp"
#include "LocalExchangeSink.hpp"
#include "ArbitaryExchanger.hpp"
#include "HashExchanger.hpp"
#include "../../Config/ExecutionConfig.hpp"
using namespace std;

class LocalExchange
{

    std::shared_ptr<LocalExchanger> exchangerSupplier;
    list<std::shared_ptr<LocalExchangeSource>> sources;
    bool allSourcesFinished = false;

    set<std::shared_ptr<LocalExchangeSink>> sinks;

    int sinksCount = 0;
    int sourcesCount = 0;

    std::shared_ptr<tbb::concurrent_queue<std::shared_ptr<DataPage>>> globalSourceBuffer = NULL;


    bool shareBuffer = false;

    int nextSource = 0;

    atomic<int> sharedBufferSourceCount = 0;


    vector<int> hashChannels;
    mutex lock;


public:

    LocalExchange(int initial_SinksCount,int initial_SourcesCount,bool shareBuffer,string type,vector<int> hashChannels)
    {

        this->sinksCount = initial_SinksCount;
        this->sourcesCount = initial_SourcesCount;

        this->hashChannels = hashChannels;

        if(shareBuffer) {

            this->shareBuffer = shareBuffer;
        }

        if(type == "hash")
            this->shareBuffer = false;

        if(!this->shareBuffer) {
            ExecutionConfig config;
            int con = atoi(config.getIntra_task_hash_build_concurrency().c_str());
            if((con & con - 1) != 0)
            {
                spdlog::warn("The value of intra-task hash build concurrency must be an integer power of 2!");
                con = 1;
            }
            this->sourcesCount = con;
            for (int i = 0; i < this->sourcesCount; i++) {
                auto newSource = this->createSource();

                this->sources.push_back(newSource);

            }

        }
        if(type != "hash")
            this->exchangerSupplier = std::make_shared<ArbitaryExchanger>(this->sources);
        else
            this->exchangerSupplier = std::make_shared<hashExchanger>(this->sources,this->hashChannels);
    }

    std::shared_ptr<LocalExchangeSource> createSource()
    {
        return std::make_shared<LocalExchangeSource>();
    }

    void closeSource(int number)
    {
        this->exchangerSupplier->closeSource(number);
    }


    std::shared_ptr<LocalExchangeSource> getNextSource()
    {

        lock.lock();
        if(this->shareBuffer) {
            if (this->globalSourceBuffer == NULL) {

                this->globalSourceBuffer = std::make_shared<tbb::concurrent_queue<std::shared_ptr<DataPage>>>();
                std::shared_ptr<LocalExchangeSource> newSource = createSourceWithGlobalBuffer();
                this->exchangerSupplier->addSource(newSource);
                lock.unlock();
                return newSource;
            }
            else
            {

                std::shared_ptr<LocalExchangeSource> newSource = createSourceWithGlobalBuffer();
                this->exchangerSupplier->addSource(newSource);
                lock.unlock();
                return newSource;
            }

        }
        lock.unlock();


        lock.lock();
        int i = 0;
        for(auto source : this->sources)
        {
            if(i == this->nextSource)
            {
               // this->exchangerSupplier->addSource(source);
                lock.unlock();
                this->nextSource++;
                return source;
            }
            else
                i++;
        }

        lock.unlock();

        if(this->nextSource >= this->sourcesCount) {
            spdlog::critical("Exceeded the fixed number of sources!");
            return NULL;
        }
        return NULL;
    }


    std::shared_ptr<LocalExchangeSource> createSourceWithGlobalBuffer()
    {

        if(this->globalSourceBuffer == NULL) {
            spdlog::critical("global source buffer is NULL!");
            return NULL;
        }
        spdlog::debug("LocalExchange create share buffer source!");
        return std::make_shared<LocalExchangeSource>(this->sharedBufferSourceCount++,this->globalSourceBuffer);

    }



    std::shared_ptr<LocalExchangeSink> createSink() {

        lock.lock();
        std::shared_ptr<LocalExchangeSink> sink = std::make_shared<LocalExchangeSink>(this->exchangerSupplier);
        this->exchangerSupplier->declareSinkCount();

        spdlog::debug("LocalExchange create sink!");
        sinks.insert(sink);
        lock.unlock();
        return sink;
    }


};








class LocalExchangeFactory {

    int bufferCount;
    int numSinkFactories;
    string exchangeType;
    vector<int> hashExchangeChannels;
    std::shared_ptr<LocalExchange> localExchange = NULL;

    mutex lock;

public:
    LocalExchangeFactory(int numSinkFactories,int bufferCount,string type) {

        this->bufferCount = bufferCount;
        this->numSinkFactories = numSinkFactories;
        this->exchangeType = type;
    }
    LocalExchangeFactory(int numSinkFactories,int bufferCount,string type,vector<int> hashChannels) {

        this->bufferCount = bufferCount;
        this->numSinkFactories = numSinkFactories;
        this->exchangeType = type;
        this->hashExchangeChannels = hashChannels;
    }

    int getBufferCount() {
        return this->bufferCount;
    }

    string getExchangeType()
    {
        return this->exchangeType;
    }

    std::shared_ptr<LocalExchange> getLocalExchange() {

        lock.lock();
        if(this->localExchange == NULL)
        {
            this->localExchange = std::make_shared<LocalExchange>(this->numSinkFactories,this->bufferCount,true,this->exchangeType,this->hashExchangeChannels);
            lock.unlock();
            return this->localExchange;
        }
        else {
            lock.unlock();
            return this->localExchange;
        }
    }

    int computeBufferCount()
    {
        int buffersCount = 3;
        /*
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            bufferCount = 1;

        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;

        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;

        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;

        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getConnectorId().isPresent()) {
            // partitioned exchange
            bufferCount = defaultConcurrency;
        }
        else {

        }
         */
        return buffersCount;
    }


};





#endif //OLVP_LOCALEXCHANGE_HPP
