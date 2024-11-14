//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_POSITIONLINKS_HPP
#define OLVP_POSITIONLINKS_HPP

#include "../../../Page/DataPage.hpp"
class PositionLinks
{

public:
    PositionLinks(){

    }


    virtual long getSizeInBytes() = 0;
    virtual int start(int position, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage) = 0;
    virtual int next(int position, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage) = 0;



};
class PositionLinksFactory
{

public:

    virtual long checksum(){return 0;};
    virtual std::shared_ptr<PositionLinks> create() = 0;
};


class PositionLinksFactoryBuilder{

public:
    PositionLinksFactoryBuilder(){}

    virtual int link(int left, int right) = 0;

    virtual std::shared_ptr<PositionLinksFactory> build() = 0;

    virtual bool isEmpty() = 0;

};

#endif //OLVP_POSITIONLINKS_HPP
