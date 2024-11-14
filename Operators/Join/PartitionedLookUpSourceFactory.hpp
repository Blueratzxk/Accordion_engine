//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_PARTITIONEDLOOKUPSOURCEFACTORY_HPP
#define OLVP_PARTITIONEDLOOKUPSOURCEFACTORY_HPP
#include "../../common.h"
#include "LookupSourceFactory.hpp"
#include "LookupSource.hpp"
#include "tbb/concurrent_hash_map.h"
#include "PartitionedLookupSource.hpp"
#include "tbb/concurrent_queue.h"

using namespace std;



class DefaultLookupSourceLease:LookupSourceLease
{
    std::shared_ptr<LookupSource> lookupSource = NULL;
public:
    DefaultLookupSourceLease(std::shared_ptr<LookupSource> lookupSource)
    {
        this->lookupSource = lookupSource;
    }

    std::shared_ptr<LookupSource> getLookupSource()
    {
        return this->lookupSource;
    }
    bool hasSpilled() {
        return NULL;
    }
    int64_t spillEpoch() {
        return 0;
    }

    ~DefaultLookupSourceLease()
    {

    }

};





class PartitionedLookupSourceFactory:public LookupSourceFactory,public enable_shared_from_this<PartitionedLookupSourceFactory>
{
    vector<std::shared_ptr<LookupSourceSupplier>> partitions;
    list<std::shared_ptr<std::promise<std::shared_ptr<LookupSourceProvider>>>> lookupSourcePromises;
    tbb::concurrent_queue<std::shared_ptr<std::promise<std::shared_ptr<LookupSourceProvider>>>> finishedLookupSourcePromises;
    tbb::concurrent_hash_map<std::shared_ptr<LookupSourceProvider>,std::shared_ptr<LookupSource>> suppliedLookupSources;
    std::shared_ptr<TrackingLookupSourceSupplier> lookupSourceSupplier;
    int partitionCount;
    atomic<int> partitionAssign = 0;
    int partitionSet = 0;
    std::shared_ptr<arrow::Schema> inputSchema;
    std::shared_ptr<arrow::Schema> outputSchema;
    vector<int> hashChannels;

    atomic<bool> lookupSourceCompleted = false;

    mutex lock2;

    mutable std::shared_mutex lock;



public:
    PartitionedLookupSourceFactory(std::shared_ptr<arrow::Schema> inputSchema,std::shared_ptr<arrow::Schema> outputSchema,int partitionCount)
    {
        this->partitionCount = partitionCount;
        this->inputSchema = inputSchema;
        this->outputSchema = outputSchema;

        for(int i = 0 ; i < this->partitionCount ;i++)
        {
            this->partitions.push_back(NULL);
        }
    }

    void resetPartitionCount()
    {
        this->partitionCount = 1;
        if (!this->partitions.empty()) {
            this->partitions.erase(this->partitions.begin() + 1, this->partitions.end());
        }
    }

    int getPartitionAssign()
    {
        return this->partitionAssign++;
    }
    std::shared_ptr<arrow::Schema> getBuildInputSchema()
    {
        return this->inputSchema;
    }


    std::shared_ptr<LookupSource> getLookupSource()
    {
        std::shared_ptr<LookupSource> source = this->lookupSourceSupplier->getLookupSource();
        return source;
    }

    std::future<std::shared_ptr<LookupSourceProvider>> createLookUpSourceProvider()
    {

        lock.lock();
        std::shared_ptr<std::promise<std::shared_ptr<LookupSourceProvider>>> promiseResult = std::make_shared<std::promise<std::shared_ptr<LookupSourceProvider>>> ();
        this->lookupSourcePromises.push_back(promiseResult);
        lock.unlock();
        return promiseResult->get_future();
    }
    shared_ptr<PartitionedLookupSourceFactory> getThisSharedPtr()
    {
        return shared_from_this();
    }

    void lendPartitionLookupSource(int partitionIndex,std::shared_ptr<LookupSourceSupplier> partitionLookupSource)
    {
        bool completed = false;


        this->lock.lock();

        this->partitions[partitionIndex] = partitionLookupSource;
        this->partitionSet++;

        if(this->partitionSet == this->partitions.size()) {
            completed = true;
        }
       this->lock.unlock();

        if(completed) {
            supplyLookupSources();
            this->lookupSourceCompleted = true;
        }


    }


    class DefaultLookupSourceProvider: public LookupSourceProvider
    {

        std::shared_ptr<PartitionedLookupSourceFactory> sourceFactory;
    public:



        DefaultLookupSourceProvider(std::shared_ptr<PartitionedLookupSourceFactory> sourceFactoryIn):LookupSourceProvider("DefaultLookupSourceProvider")
        {
            this->sourceFactory = sourceFactoryIn;
        }

        template<typename T,typename R>
        R withLease(T visitorIn,
                    function<R(T visitor,std::shared_ptr<DefaultLookupSourceLease> lease)> action) {

            this->sourceFactory->lock.lock_shared();
            shared_ptr<LookupSource> lookupSourcePtr = this->sourceFactory->getLookupSource();
            std::shared_ptr<DefaultLookupSourceLease> leaseSource = std::make_shared<DefaultLookupSourceLease>(lookupSourcePtr);
            R returnValue = action(visitorIn,leaseSource);
            this->sourceFactory->lock.unlock_shared();

            return returnValue;
        }

        bool isLookupSourceExist()
        {
            return this->sourceFactory->getLookupSource() != NULL?true:false;
        }


    };


    void tryGetCompletedLookupSource()
    {
        if(this->lookupSourceCompleted){
            lock.lock();

            for(auto promise = this->lookupSourcePromises.begin() ; promise != this->lookupSourcePromises.end() ; ++ promise)
            {
                (*promise)->set_value(std::make_shared<DefaultLookupSourceProvider>(this->getThisSharedPtr()));
            }
            this->lookupSourcePromises.clear();
            lock.unlock();
        }
    }

    void supplyLookupSources()
    {
        lock.lock();
        if(this->partitionCount == 1)
        {
           std::shared_ptr<simpleTrackingLookupSourceSupplier> supplier = std::make_shared<simpleTrackingLookupSourceSupplier>(this->partitions[0]);
           this->lookupSourceSupplier = supplier;
        }
        else
        {
            std::shared_ptr<PartitionedLookupSource::PatitionedTrackingLookupSourceSupplier> supplier =
                    std::make_shared<PartitionedLookupSource::PatitionedTrackingLookupSourceSupplier>(this->partitions);
            this->lookupSourceSupplier = supplier;
        }

        for(auto promise = this->lookupSourcePromises.begin() ; promise != this->lookupSourcePromises.end() ; ++ promise)
        {
            (*promise)->set_value(std::make_shared<DefaultLookupSourceProvider>(this->getThisSharedPtr()));
        }


        this->lookupSourcePromises.clear();


        lock.unlock();


    }



};


#endif //OLVP_PARTITIONEDLOOKUPSOURCEFACTORY_HPP
