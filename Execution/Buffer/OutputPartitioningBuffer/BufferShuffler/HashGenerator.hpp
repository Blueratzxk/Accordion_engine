//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_HASHGENERATOR_HPP
#define OLVP_HASHGENERATOR_HPP

//#include "../../../../Page/DataPage.hpp"
class DataPage;
class HashGenerator
{
public:

    virtual long hashPosition(int position,shared_ptr<DataPage> page) = 0;
    inline int hashCode(long value) {

        return (int)(((unsigned long)value) ^ (((unsigned long)value) >> 32));
    }

    inline int getPartition2(int partitionCount, int position, shared_ptr<DataPage> page)
    {
        long rawHash = hashPosition(position, page);

        // This function reduces the 64 bit rawHash to [0, partitionCount) uniformly. It first reduces the rawHash to 32 bit
        // integer x then normalize it to x / 2^32 * partitionCount to reduce the range of x from [0, 2^32) to [0, partitionCount)
        return (int) ((((unsigned long)hashCode(rawHash)) * partitionCount) >> 32);
    }

    inline int getPartition(int partitionCount, int position, shared_ptr<DataPage> page)
    {
        long rawHash = hashPosition(position, page);

        // This function reduces the 64 bit rawHash to [0, partitionCount) uniformly. It first reduces the rawHash to 32 bit
        // integer x then normalize it to x / 2^32 * partitionCount to reduce the range of x from [0, 2^32) to [0, partitionCount)
        return (int) ((((unsigned long)hashCode(rawHash))%partitionCount));
    }

    inline int getPartition(int partitionCount,long rawHash)
    {
        // This function reduces the 64 bit rawHash to [0, partitionCount) uniformly. It first reduces the rawHash to 32 bit
        // integer x then normalize it to x / 2^32 * partitionCount to reduce the range of x from [0, 2^32) to [0, partitionCount)
        return (int) ((((unsigned long)hashCode(rawHash))%partitionCount));
    }


    inline int getPartition2(int partitionCount,int maxPositionCount, int position,  shared_ptr<DataPage> page)
    {
        long rawHash = hashPosition(position, page)%maxPositionCount;

        // This function reduces the 64 bit rawHash to [0, partitionCount) uniformly. It first reduces the rawHash to 32 bit
        // integer x then normalize it to x / 2^32 * partitionCount to reduce the range of x from [0, 2^32) to [0, partitionCount)
        return (int) ((((unsigned long)hashCode(rawHash)) * partitionCount) >> 32);
    }

};


#endif //OLVP_HASHGENERATOR_HPP
