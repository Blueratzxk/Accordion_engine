//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_HASHCOMMON_HPP
#define OLVP_HASHCOMMON_HPP


#include "math.h"

class HashCommon {
private:
    int INT_PHI = -1640531527;
    int INV_INT_PHI = 340573321;
    long LONG_PHI = -7046029254386353131L;
    long INV_LONG_PHI = -1018231460777725123L;


public:

    HashCommon() {}

    int murmurHash3(unsigned int x) {
        x ^= x >> 16;
        x *= -2048144789;
        x ^= x >> 13;
        x *= -1028477387;
        x ^= x >> 16;
        return x;
    }

    long murmurHash3(unsigned long x) {
        x ^= x >> 33;
        x *= -49064778989728563L;
        x ^= x >> 33;
        x *= -4265267296055464877L;
        x ^= x >> 33;
        return x;
    }

    int mix(unsigned int x) {
        int h = x * -1640531527;
        return h ^ h >> 16;
    }

    int invMix(unsigned  int x) {
        return (x ^ x >> 16) * 340573321;
    }

    long mix(unsigned long x) {
        long h = x * -7046029254386353131L;
        h ^= h >> 32;
        return h ^ h >> 16;
    }

    long invMix(unsigned long x) {
        x ^= x >> 32;
        x ^= x >> 16;
        return (x ^ x >> 32) * -1018231460777725123L;
    }



    int long2int(unsigned  long l) {
        return (int)(l ^ l >> 32);
    }

    int nextPowerOfTwo(int x) {
        if (x == 0) {
            return 1;
        } else {
            --x;
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            return (x | x >> 16) + 1;
        }
    }

    int combineHash(int previousHashValue, int value)
    {
        return (31 * previousHashValue + value);
    }

    long nextPowerOfTwo(long x) {
        if (x == 0L) {
            return 1L;
        } else {
            --x;
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            return (x | x >> 32) + 1L;
        }
    }

    long max(long left,long right)
    {
        return left > right ? left :right;
    }


    int arraySize(int expected, float f) {

        long s = max(2L, nextPowerOfTwo((long)ceil((double)((float)expected / f))));
        if (s > 1073741824L) {
            return  -1;
        } else {
            return (int)s;
        }
    }

   long bigArraySize(long expected, float f) {
        return nextPowerOfTwo((long)ceil((double)((float)expected / f)));
    }
};


#endif //OLVP_HASHCOMMON_HPP
