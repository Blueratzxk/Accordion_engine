//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_POSITIONLINKS_HPP
#define OLVP_POSITIONLINKS_HPP

#include "../../../Page/DataPage.hpp"
class PositionLinks
{

    string type;
public:
    PositionLinks(string type) {
        this->type = type;
    }

    string getType()
    {
        return this->type;
    }


    virtual long getSizeInBytes() = 0;
    virtual int start(int position, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage) = 0;
    virtual int next(int position, int probePosition, std::shared_ptr<DataPage> allProbeChannelsPage) = 0;



};
class PositionLinksFactory
{

    string type;
public:
    PositionLinksFactory(string type){
        this->type = type;
    }
    string getType()
    {
        return this->type;
    }
    virtual long checksum(){return 0;};
    virtual std::shared_ptr<PositionLinks> create() = 0;
};


class PositionLinksFactoryBuilder{

    string type;
public:
    PositionLinksFactoryBuilder(string type){this->type = type;}

    string getType()
    {
        return this->type;
    }

    virtual int link(int left, int right) = 0;

    virtual std::shared_ptr<PositionLinksFactory> build() = 0;

    virtual bool isEmpty() = 0;

    virtual void setPositionLinks(shared_ptr<int> positionLinks) = 0;

    virtual std::shared_ptr<int> getPositionLinks() = 0;

};

#endif //OLVP_POSITIONLINKS_HPP
