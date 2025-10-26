//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_JOINHASHSUPPLIER_HPP
#define OLVP_JOINHASHSUPPLIER_HPP

#include "PagesHash.hpp"
#include "PagesHashStrategy.hpp"
#include "ArrayPositionLinks.hpp"
#include "JoinHash.hpp"
#include "../LookupSourceSupplier.hpp"
#include "../BuildComponents/HashJoinBuildComponents.hpp"
#include "../BuildComponents/JoinHashComponent.hpp"
class JoinHashSupplier:public LookupSourceSupplier
{
    std::shared_ptr<PagesHash> pagesHash = NULL;
    list<std::shared_ptr<DataPage>> pages;
    std::shared_ptr<PagesHashStrategy> hashStrategy = NULL ;
    std::shared_ptr<PositionLinksFactory> positionLinksFactory = NULL;

    shared_ptr<JoinBuildComponents> joinBuildComponentsForInstall;
public:
    JoinHashSupplier(std::shared_ptr<PagesHashStrategy> hashStrategy,int positionCount,atomic<long> &buildProgress): LookupSourceSupplier("JoinHashSupplier")
    {

        std::shared_ptr<ArrayPositionLinksFactoryBuilder> positionLinksFactoryBuilder = std::make_shared<ArrayPositionLinksFactoryBuilder>(positionCount);
        this->pagesHash = std::make_shared<PagesHash>(positionCount,hashStrategy,positionLinksFactoryBuilder,buildProgress);
        this->positionLinksFactory = positionLinksFactoryBuilder->build();
    }

    JoinHashSupplier(std::shared_ptr<PagesHashStrategy> hashStrategy,int positionCount,atomic<long> &buildProgress,shared_ptr<JoinBuildComponents> joinBuildComponents): LookupSourceSupplier("JoinHashSupplier")
    {

        this->joinBuildComponentsForInstall = joinBuildComponents;
        std::shared_ptr<ArrayPositionLinksFactoryBuilder> positionLinksFactoryBuilder = std::make_shared<ArrayPositionLinksFactoryBuilder>(positionCount);

        auto components = static_pointer_cast<JoinHashComponent>(joinBuildComponents);

        this->pagesHash = std::make_shared<PagesHash>(components->getPositionCount(),components->getHashSize(),hashStrategy,positionLinksFactoryBuilder,buildProgress,
                                                      components->getPartitionToHashKeyArray(),components->getPartitionToPositionToHashes(),
                                                      components->getPartitionToPositionLinks());
        this->positionLinksFactory = positionLinksFactoryBuilder->build();
    }
    int getPositionCount()
    {
        return this->pagesHash->getPositionCount();
    }
    int getHashSize()
    {
        return this->pagesHash->getHashSize();
    }
    shared_ptr<int> getHashKeyArray()
    {
        return this->pagesHash->getHashKeyArray();
    }

    shared_ptr<uint8_t > getPositionToHashes()
    {
        return this->pagesHash->getPositionToHashes();
    }

    shared_ptr<int> getPositionLink()
    {
        if(this->positionLinksFactory->getType() == "ArrayPositionLinksFactory"){
            auto pl = static_pointer_cast<ArrayPositionLinksFactory>(this->positionLinksFactory)->create();
            if(pl->getType() == "ArrayPositionLinks")
                return static_pointer_cast<ArrayPositionLinks>(pl)->getPositionLinks();
            else
                return NULL;
        }
        else
            return NULL;
    }


    shared_ptr<JoinBuildComponents> getBuildComponents(int partitionId) {

        auto component = make_shared<JoinHashComponent>(partitionId,this->getHashSize(),this->getPositionCount());
        component->addPartitionToHashKeyArray(this->getHashKeyArray());
        component->addParitionToPositionToHashes(this->getPositionToHashes());
        component->addPartitionToPositionLinks(this->getPositionLink());

        return component;
    }

    std::shared_ptr<LookupSource> get()
    {
        std::shared_ptr<JoinHash> joinHash = std::make_shared<JoinHash>(this->pagesHash,this->positionLinksFactory->create());
        return joinHash;
    }


};

#endif //OLVP_JOINHASHSUPPLIER_HPP
