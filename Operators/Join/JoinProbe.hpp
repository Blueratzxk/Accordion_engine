//
// Created by zxk on 5/25/23.
//

#ifndef OLVP_JOINPROBE_HPP
#define OLVP_JOINPROBE_HPP

#include "../../Page/DataPage.hpp"
#include "LookupSource.hpp"
class JoinProbe
{

    vector<int> probeOutputChannels;
    int positionCount = 0;
    std::shared_ptr<DataPage> allPage;
    std::shared_ptr<DataPage> probePage;
    int probePosition = -1;

public:


    JoinProbe(vector<int> probeOutputChannels,std::shared_ptr<DataPage> allPage,std::shared_ptr<DataPage> probePage)
    {

        this->probeOutputChannels = probeOutputChannels;
        this->allPage = allPage;
        this->probePage = probePage;
        this->positionCount = allPage->getElementsCount();

    }

    vector<int> getOutputChannels()
    {
        return this->probeOutputChannels;
    }

    bool advanceNextProbePostion()
    {
        return ++this->probePosition < this->positionCount;
    }

    int64_t getCurrentJoinPosition(std::shared_ptr<LookupSource> lookupSource)
    {
        return lookupSource->getJoinPosition(this->probePosition,probePage,allPage);
    }
    int getProbePosition()
    {
        return this->probePosition;
    }

    std::shared_ptr<DataPage> getPage()
    {
        return this->allPage;
    }


    bool currentRowContainsNull()
    {
       return false;
    }

    static bool probeMayHaveNull()
    {
       return false;
    }


};

class JoinProbeFactory
{
    vector<int> probeOutputChannels;
    vector<int> probeJoinChannels;

public:

    JoinProbeFactory(vector<int> probeOutputChannels,vector<int> probeJoinChannels)
    {
        this->probeOutputChannels = probeOutputChannels;
        this->probeJoinChannels = probeJoinChannels;
    }

    std::shared_ptr<JoinProbe> createJoinProbe(std::shared_ptr<DataPage> page)
    {
        return std::make_shared<JoinProbe>(probeOutputChannels,page,page->getLoadedPage(probeJoinChannels));
    }



};


#endif //OLVP_JOINPROBE_HPP
