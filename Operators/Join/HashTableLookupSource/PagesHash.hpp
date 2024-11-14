//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_PAGESHASH_HPP
#define OLVP_PAGESHASH_HPP

#include "../LookupSourceSupplier.hpp"
#include "../../../Page/DataPage.hpp"
#include "../../../Page/Channel.hpp"
#include "PagesHashStrategy.hpp"
#include "../../../Utils/HashCommon.hpp"
#include "PositionLinks.hpp"

class PagesHash
{
    int positionCount;
    std::shared_ptr<PagesHashStrategy> pagesHashStrategy;

    int hashSize;
    int mask;

    std::shared_ptr<int> hashKeyArray;

    std::shared_ptr<uint8_t>  positionToHashes;

    std::shared_ptr<PositionLinksFactoryBuilder> positionLinks;

public:
    PagesHash(int positionCount,std::shared_ptr<PagesHashStrategy> pagesHashStrategy,std::shared_ptr<PositionLinksFactoryBuilder> positionLinks)
    {
        this->positionCount = positionCount;
        this->pagesHashStrategy = pagesHashStrategy;
        this->positionLinks = positionLinks;
        this->createHashTable();

    }

    void createHashTable()
    {
        HashCommon common;
        this->hashSize = common.arraySize(this->positionCount,0.75f);
        this->mask = this->hashSize - 1;

        this->hashKeyArray = shared_ptr<int> (new int[hashSize],[](int *p){delete [] p;});
        memset((int*)this->hashKeyArray.get(),-1,this->hashSize*sizeof(int));

        this->positionToHashes = shared_ptr<uint8_t> (new uint8_t[hashSize],[](uint8_t *p){delete [] p;});

        std::shared_ptr<int64_t> positionToFullHashes = shared_ptr<int64_t> (new int64_t[hashSize],[](int64_t *p){delete [] p;});

        for(int position = 0 ; position < this->positionCount ; position++)
        {
            int64_t hash  = this->pagesHashStrategy->hashPosition(position);
            positionToFullHashes.get()[position] = hash;
            positionToHashes.get()[position] = (uint8_t)hash;
        }



        for(int position = 0 ; position < this->positionCount ; position++)
        {

            int positionTmp = position;

            int64_t hash = positionToFullHashes.get()[positionTmp];
            int pos = PagesHash::getHashPosition(hash,this->mask);

            while(this->hashKeyArray.get()[pos] != -1)
            {
                int currentKey = this->hashKeyArray.get()[pos];

                if((uint8_t)hash == this->positionToHashes.get()[currentKey] && this->pagesHashStrategy->positionEqualsPosition(currentKey,positionTmp))
                {
                    positionTmp = this->positionLinks->link(position,currentKey);

                    break;
                }
                pos = (pos + 1) & mask;

            }


            this->hashKeyArray.get()[pos] = positionTmp;
        }


    }



    int getAddressIndex(int position, std::shared_ptr<DataPage> hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy->hashRow(position, hashChannelsPage));
    }

    int getAddressIndex(int rightPosition, std::shared_ptr<DataPage> hashChannelsPage, long rawHash)
    {
        int pos = PagesHash::getHashPosition(rawHash, mask);

        while (this->hashKeyArray.get()[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(this->hashKeyArray.get()[pos], (uint8_t) rawHash, rightPosition, hashChannelsPage)) {
                return this->hashKeyArray.get()[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    bool positionEqualsCurrentRowIgnoreNulls(int leftPosition, uint8_t rawHash, int rightPosition, std::shared_ptr<DataPage> rightPage)
    {
        if (this->positionToHashes.get()[leftPosition] != rawHash) {
            return false;
        }

        return pagesHashStrategy->positionEqualsRowIgnoreNulls(leftPosition,rightPosition, rightPage);
    }

    void appendTo(long position, std::shared_ptr<DataPageBuilder> pageBuilder, int outputChannelOffset)
    {
        pagesHashStrategy->appendTo( position, pageBuilder, outputChannelOffset);
    }

    int getChannelCount()
    {
        return this->pagesHashStrategy->getChannelCount();
    }

    int getPositionCount()
    {
        return this->positionCount;
    }

    static int getHashPosition(uint64_t rawHash, uint64_t mask)
    {
        rawHash ^= rawHash >> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >> 33;

        return (int32_t) (rawHash & mask);
    }


};
#endif //OLVP_PAGESHASH_HPP
