//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_LOOKUPSOURCEFACTORY_HPP
#define OLVP_LOOKUPSOURCEFACTORY_HPP

#include "JoinBridge.hpp"
#include "LookupSource.hpp"
#include <iostream>
#include <future>
#include "LookupSourceProvider.hpp"

class LookupSourceFactory:public JoinBridge
{
public:

    virtual std::future<std::shared_ptr<LookupSourceProvider>> createLookUpSourceProvider() = 0;
    virtual void tryGetCompletedLookupSource() = 0;
};
#endif //OLVP_LOOKUPSOURCEFACTORY_HPP
