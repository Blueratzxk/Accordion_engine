//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_LOOKUPSOURCE_HPP
#define OLVP_LOOKUPSOURCE_HPP

#include <string>
#include "arrow/builder.h"
#include "../../Page/DataPage.hpp"
#include "../../Page/DataPageBuilder.hpp"
class LookupSource{

    std::string lookupSourceId;
public:
    LookupSource(std::string id){
        this->lookupSourceId = id;
    }

    virtual int32_t getChannelCount() = 0;

    virtual int64_t getInMemorySizeInBytes() = 0;

    virtual int64_t getJoinPositionCount() = 0;

    virtual int64_t joinPositionWithinPartition(int64_t joinPosition) = 0;

    virtual int64_t getJoinPosition(int32_t position, std::shared_ptr<DataPage> hashChannelsPage, std::shared_ptr<DataPage> allChannelsPage, int64_t rawHash) = 0;

    virtual int64_t getJoinPosition(int32_t position, std::shared_ptr<DataPage> hashChannelsPage, std::shared_ptr<DataPage> allChannelsPage) = 0;

    virtual int64_t getNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition, std::shared_ptr<DataPage> allProbeChannelsPage) = 0;

    virtual void appendTo(int64_t position,   std::shared_ptr<DataPageBuilder> builder, int32_t outputChannelOffset) = 0;

    virtual bool isJoinPositionEligible(int64_t currentJoinPosition, int32_t probePosition,  std::shared_ptr<DataPage> allProbeChannelsPage) = 0;

    virtual bool isEmpty() = 0;


};


#endif //OLVP_LOOKUPSOURCE_HPP
