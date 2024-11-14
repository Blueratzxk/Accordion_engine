//
// Created by zxk on 6/7/23.
//

#ifndef OLVP_RANDOM_HPP
#define OLVP_RANDOM_HPP
#include <random>
#include <iostream>

class RandomNumber {

public:
    static unsigned long long get(long min,long max) {
        std::random_device rd;     //Get a random seed from the OS entropy device, or whatever
        std::mt19937_64 eng(rd()); //Use the 64-bit Mersenne Twister 19937 generator
        std::uniform_int_distribution<unsigned long long> distr(min, max);
        return distr(eng);
    }

    static int getInt(int min,int max) {
        std::random_device rd;     //Get a random seed from the OS entropy device, or whatever
        std::mt19937_64 eng(rd()); //Use the 64-bit Mersenne Twister 19937 generator
        std::uniform_int_distribution<int> distr(min, max);
        return distr(eng);
    }
};
#endif //OLVP_RANDOM_HPP
