//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_ARRAYPOSITIONLINKS_HPP
#define OLVP_ARRAYPOSITIONLINKS_HPP

#include "PositionLinks.hpp"






class ArrayPositionLinks:public PositionLinks
{
    std::shared_ptr<int> positionLinks;

public:

    ArrayPositionLinks(std::shared_ptr<int> positionLinks)
    {
        this->positionLinks = positionLinks;
    }

    long getSizeInBytes(){return 0;}

    int start(int position, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage)
    {
        return position;
    }
    int next(int position, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage)
    {
        return positionLinks.get()[position];
    }

};


class ArrayPositionLinksFactory:public PositionLinksFactory
{
    std::shared_ptr<int> positionLinks;
public:
    ArrayPositionLinksFactory(std::shared_ptr<int> positionLinks)
    {
        this->positionLinks = positionLinks;
    }

    std::shared_ptr<PositionLinks> create()
    {

        return std::make_shared<ArrayPositionLinks>(positionLinks);
    }
};


class ArrayPositionLinksFactoryBuilder:public PositionLinksFactoryBuilder
{
    std::shared_ptr<int> positionLinks;
    int size;

public:
    ArrayPositionLinksFactoryBuilder(int size)
    {
        this->positionLinks = shared_ptr<int32_t> (new int32_t[size],[](int32_t *p){delete [] p;});
        memset((int*)this->positionLinks.get(),-1,size*sizeof(int));
        this->size = size;
    }
    int link(int left, int right) override
    {
        this->size++;
        this->positionLinks.get()[left] = right;
        return left;
    }


    std::shared_ptr<PositionLinksFactory> build() override
    {

        return std::make_shared<ArrayPositionLinksFactory>(this->positionLinks);
    }

    bool isEmpty() override
    {
        return size == 0;
    }
};


#endif //OLVP_ARRAYPOSITIONLINKS_HPP
