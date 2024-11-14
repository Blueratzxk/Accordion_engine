//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_LOOKUPSOURCEPROVIDER_HPP
#define OLVP_LOOKUPSOURCEPROVIDER_HPP

#include "LookupSource.hpp"
#include "LookupSourceFactory.hpp"
#include <iostream>
#include <future>

class LookupSourceLease
{
public:

    virtual std::shared_ptr<LookupSource> getLookupSource() = 0;
    virtual bool hasSpilled() = 0;
    virtual int64_t spillEpoch() = 0;

};

class LookupSourceProvider
{
    string id;
public:
    LookupSourceProvider(string id){
        this->id = id;
    }
    string getId()
    {
        return this->id;

    }
    virtual bool isLookupSourceExist() = 0;


};

#endif //OLVP_LOOKUPSOURCEPROVIDER_HPP
