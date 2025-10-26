//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_LOOKUPSOURCESUPPLIER_HPP
#define OLVP_LOOKUPSOURCESUPPLIER_HPP

#include <string>
#include "BuildComponents/JoinBuildComponents.hpp"
class LookupSourceSupplier
{
    std::string id;
public:
    LookupSourceSupplier(std::string id)
    {
        this->id = id;
    }
    std::string  getId()
    {
        return this->id;
    }

    virtual std::shared_ptr<LookupSource> get() = 0;

    virtual  shared_ptr<JoinBuildComponents> getBuildComponents(int partitionId){return NULL;}
};

#endif //OLVP_LOOKUPSOURCESUPPLIER_HPP
