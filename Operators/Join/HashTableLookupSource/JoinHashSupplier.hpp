//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_JOINHASHSUPPLIER_HPP
#define OLVP_JOINHASHSUPPLIER_HPP

#include "PagesHash.hpp"
#include "PagesHashStrategy.hpp"
#include "ArrayPositionLinks.hpp"
#include "JoinHash.hpp"
class JoinHashSupplier:public LookupSourceSupplier
{
    std::shared_ptr<PagesHash> pagesHash = NULL;
    list<std::shared_ptr<DataPage>> pages;
    std::shared_ptr<PagesHashStrategy> hashStrategy = NULL ;
    std::shared_ptr<PositionLinksFactory> positionLinksFactory = NULL;
public:
    JoinHashSupplier(std::shared_ptr<PagesHashStrategy> hashStrategy,int positionCount,vector<Channel> channels): LookupSourceSupplier("JoinHashSupplier")
    {

        std::shared_ptr<ArrayPositionLinksFactoryBuilder> positionLinksFactoryBuilder = std::make_shared<ArrayPositionLinksFactoryBuilder>(positionCount);
        this->pagesHash = std::make_shared<PagesHash>(positionCount,hashStrategy,positionLinksFactoryBuilder);
        this->positionLinksFactory = positionLinksFactoryBuilder->build();
    }

    std::shared_ptr<LookupSource> get()
    {
        std::shared_ptr<JoinHash> joinHash = std::make_shared<JoinHash>(this->pagesHash,this->positionLinksFactory->create());
        return joinHash;
    }


};

#endif //OLVP_JOINHASHSUPPLIER_HPP
