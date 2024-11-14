//
// Created by zxk on 5/24/23.
//

#ifndef OLVP_PARTITIONEDLOOKUPSOURCE_HPP
#define OLVP_PARTITIONEDLOOKUPSOURCE_HPP

#include "LookupSource.hpp"
#include "TrackingLookupSourceSupplier.hpp"
#include "../../Execution/Buffer/OutputPartitioningBuffer/BufferShuffler/SimplePageHashGenerator.hpp"
class PatitionedLookupSourceSupplier;

class PartitionedLookupSource : public LookupSource
{
    vector<std::shared_ptr<LookupSource>> lookupSources;
    int partitionMask;
    int shiftSize;
    shared_ptr<SimplePageHashGenerator> hashGen;

    int decodePartition(long partitionedJoinPosition)
    {
        return (int) (partitionedJoinPosition & partitionMask);
    }

    int toIntExact(long value) {
        if ((int)value != value) {
            return -1;
        }
        return (int)value;
    }


    int decodeJoinPosition(long partitionedJoinPosition)
    {
        return toIntExact((unsigned long)partitionedJoinPosition >> shiftSize);
    }

    long encodePartitionedJoinPosition(int partition, int joinPosition)
    {
        return (((long) joinPosition) << shiftSize) | (partition);
    }
    int numberOfTrailingZeros(long i) {
        // HD, Figure 5-14
        int x, y;
        if (i == 0) return 64;
        int n = 63;
        y = (int)i; if (y != 0) { n = n -32; x = y; } else x = (int)(((unsigned long)i)>>32);
        y = x <<16; if (y != 0) { n = n -16; x = y; }
        y = x << 8; if (y != 0) { n = n - 8; x = y; }
        y = x << 4; if (y != 0) { n = n - 4; x = y; }
        y = x << 2; if (y != 0) { n = n - 2; x = y; }
        return n - (((unsigned int)(x << 1)) >> 31);
    }
public:
    PartitionedLookupSource(vector<std::shared_ptr<LookupSource>> lookupSources):LookupSource("PartitionedLookupSource"){
        this->lookupSources = lookupSources;
        this->partitionMask = lookupSources.size() - 1;
        this->shiftSize = numberOfTrailingZeros(lookupSources.size()) + 1;

    }

    int32_t getChannelCount(){
        return lookupSources[0]->getChannelCount();
    };

    int64_t getInMemorySizeInBytes() {
        return 0;
    };

    int64_t getJoinPositionCount() {

        int64_t sum = 0;
        for(auto source: lookupSources)
        {
            sum += source->getJoinPositionCount();
        }
        return sum;
    };



    int64_t joinPositionWithinPartition(int64_t joinPosition) { return 0;}

    int64_t getJoinPosition(int32_t position, std::shared_ptr<DataPage> hashChannelsPage, std::shared_ptr<DataPage> allChannelsPage, int64_t rawHash) {

        int partition = hashGen->getPartition(this->lookupSources.size(),rawHash);

        auto lookupSource = lookupSources[partition];
        long joinPosition = lookupSource->getJoinPosition(position, hashChannelsPage, allChannelsPage);

    //    int64_t  en = encodePartitionedJoinPosition(partition, toIntExact(joinPosition));
   //     spdlog::info(to_string(partition)+"|" + to_string(toIntExact(joinPosition))+ "|" +to_string(en)+"$"+to_string(decodeJoinPosition(en)) + "|" + to_string(decodePartition(en)));


        if (joinPosition < 0) {
            return joinPosition;
        }



        return encodePartitionedJoinPosition(partition, toIntExact(joinPosition));

    }

    int64_t getJoinPosition(int32_t position, std::shared_ptr<DataPage> hashChannelsPage, std::shared_ptr<DataPage> allChannelsPage) {
        return this->getJoinPosition(position, hashChannelsPage, allChannelsPage, hashGen->hashPositionForProbeHashChannelsPage(position,hashChannelsPage));

    }

    int64_t getNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition, std::shared_ptr<DataPage> allProbeChannelsPage) {
        int partition = decodePartition(currentJoinPosition);
        long joinPosition = decodeJoinPosition(currentJoinPosition);
        auto lookupSource = lookupSources[partition];
        long nextJoinPosition = lookupSource->getNextJoinPosition(joinPosition, probePosition, allProbeChannelsPage);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }
        return encodePartitionedJoinPosition(partition, toIntExact(nextJoinPosition));

    }

    void appendTo(int64_t position,   std::shared_ptr<DataPageBuilder> builder, int32_t outputChannelOffset) {
        int partition = decodePartition(position);
        int joinPosition = decodeJoinPosition(position);
        lookupSources[partition]->appendTo(joinPosition, builder, outputChannelOffset);
      //  if (outerPositionTracker != null) {
       //     outerPositionTracker.positionVisited(partition, joinPosition);
       // }
    }

    bool isJoinPositionEligible(int64_t currentJoinPosition, int32_t probePosition,  std::shared_ptr<DataPage> allProbeChannelsPage) {

        int partition = decodePartition(currentJoinPosition);
        long joinPosition = decodeJoinPosition(currentJoinPosition);
        auto lookupSource = lookupSources[partition];
        return lookupSource->isJoinPositionEligible(joinPosition, probePosition, allProbeChannelsPage);

      //  return true;
    }

    bool isEmpty() {

        for(auto source : this->lookupSources)
        {
            if(!source->isEmpty())
                return false;
        }
        return true;
    }



    class PatitionedTrackingLookupSourceSupplier:public TrackingLookupSourceSupplier
    {

    public:
        vector<std::shared_ptr<LookupSource>> partitions;
        PatitionedTrackingLookupSourceSupplier(vector<std::shared_ptr<LookupSourceSupplier>> paritions)
        {
            for(int i = 0 ; i < paritions.size() ; i++)
            {
                if(paritions[i] != NULL)
                    this->partitions.push_back(paritions[i]->get());
                else
                    this->partitions.push_back(NULL);
            }
        }
        std::shared_ptr<LookupSource> getLookupSource()
        {
            return std::make_shared<PartitionedLookupSource> (partitions);
        }
    };


    static std::shared_ptr<PatitionedTrackingLookupSourceSupplier> createPartitionedLookupSourceSupplier(vector<std::shared_ptr<LookupSourceSupplier>> paritions)
    {
        return std::make_shared<PatitionedTrackingLookupSourceSupplier>(paritions);
    }

};




#endif //OLVP_PARTITIONEDLOOKUPSOURCE_HPP
