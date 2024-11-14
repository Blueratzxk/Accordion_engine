//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_JOINHASH_HPP
#define OLVP_JOINHASH_HPP

#include "../LookupSource.hpp"
#include "PagesHash.hpp"
#include "PositionLinks.hpp"

class JoinHash:public LookupSource
{
    std::shared_ptr<PagesHash> pagesHash;
    std::shared_ptr<PositionLinks> positionLinks;

public:
    JoinHash(std::shared_ptr<PagesHash> pagesHash,std::shared_ptr<PositionLinks> positionLinks): LookupSource("JoinHash")
    {
        this->pagesHash = pagesHash;
        this->positionLinks = positionLinks;
    }


    inline bool isEmpty()
    {
        return getJoinPositionCount() == 0;
    }
    inline int getChannelCount()
    {
        return pagesHash->getChannelCount();
    }

    inline long getJoinPositionCount()
    {
        return pagesHash->getPositionCount();
    }

    inline long getInMemorySizeInBytes()
    {
       return 0;
    }

    inline long joinPositionWithinPartition(long joinPosition)
    {
        return joinPosition;
    }

    inline long getJoinPosition(int position, std::shared_ptr<DataPage> hashChannelsPage, std::shared_ptr<DataPage> allChannelsPage)
    {
        int addressIndex = pagesHash->getAddressIndex(position, hashChannelsPage);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }

    inline long getJoinPosition(int32_t position, std::shared_ptr<DataPage> hashChannelsPage, std::shared_ptr<DataPage> allChannelsPage, int64_t rawHash)
    {
        int addressIndex = pagesHash->getAddressIndex(position, hashChannelsPage, rawHash);
        return startJoinPosition(addressIndex, position, allChannelsPage);
    }

    inline long startJoinPosition(int currentJoinPosition, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage)
    {
        if (currentJoinPosition == -1) {
            return -1;
        }
        if (this->positionLinks == NULL) {
            return currentJoinPosition;
        }
        return this->positionLinks->start(currentJoinPosition, probePosition, allProbeChannelsPage);
    }

    inline long getNextJoinPosition(long currentJoinPosition, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage)
    {
        if (this->positionLinks == NULL) {
            return -1;
        }
        return positionLinks->next(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);
    }

    inline bool isJoinPositionEligible(long currentJoinPosition, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage)
    {
       // return filterFunction == null || filterFunction.filter(toIntExact(currentJoinPosition), probePosition, allProbeChannelsPage);


       return true;
    }
    inline void appendTo(long position, std::shared_ptr<DataPageBuilder> pageBuilder, int outputChannelOffset)
    {
        pagesHash->appendTo(toIntExact(position), pageBuilder, outputChannelOffset);
    }


    inline int toIntExact(long value) {
        if ((int)value != value) {
            spdlog::critical("JoinHash position integer overFlow!");
            return -1;
        }
        return (int)value;
    }


};

#endif //OLVP_JOINHASH_HPP
