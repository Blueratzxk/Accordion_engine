//
// Created by zxk on 5/23/23.
//

#ifndef OLVP_TRACKINGLOOKUPSOURCESUPPLIER_HPP
#define OLVP_TRACKINGLOOKUPSOURCESUPPLIER_HPP

#include "LookupSource.hpp"



class TrackingLookupSourceSupplier
{
public:
    virtual std::shared_ptr<LookupSource> getLookupSource() = 0;

};


class simpleTrackingLookupSourceSupplier:public TrackingLookupSourceSupplier
{
    std::shared_ptr<LookupSourceSupplier> lookupSource;
public:
    simpleTrackingLookupSourceSupplier(std::shared_ptr<LookupSourceSupplier> lookupSource)
    {
        this->lookupSource = lookupSource;
    }
    std::shared_ptr<LookupSource> getLookupSource(){
        if(this->lookupSource != NULL)
            return this->lookupSource->get();
        else
            return NULL;
    }

};


#endif //OLVP_TRACKINGLOOKUPSOURCESUPPLIER_HPP
