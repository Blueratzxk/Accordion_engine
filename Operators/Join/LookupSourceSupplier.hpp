//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_LOOKUPSOURCESUPPLIER_HPP
#define OLVP_LOOKUPSOURCESUPPLIER_HPP

#include <string>

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
};

#endif //OLVP_LOOKUPSOURCESUPPLIER_HPP
