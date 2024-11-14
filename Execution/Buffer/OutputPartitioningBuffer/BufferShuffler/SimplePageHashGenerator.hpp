//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_SIMPLEPAGEHASHGENERATOR_HPP
#define OLVP_SIMPLEPAGEHASHGENERATOR_HPP


#include "HashGenerator.hpp"
#include "../../../../Utils/TypeUtils.hpp"
#include "../../../../Utils/HashCommon.hpp"
class SimplePageHashGenerator:public HashGenerator
{
    vector<int> hashChannels;

public:

    SimplePageHashGenerator(vector<int> hashChannels){

        this->hashChannels = hashChannels;
    }
    SimplePageHashGenerator(){
    }

    inline long hashPosition(int position, shared_ptr<DataPage> page)
    {

        HashCommon hashCommon;
        long result = 0;
        for (int i = 0; i < hashChannels.size(); i++) {
            shared_ptr<arrow::DataType> type = page->get()->column(hashChannels[i])->type();
            int re = TypeUtils::hashPosition(type,page->get()->column(hashChannels[i]),position);
            result = hashCommon.combineHash(result,re);
        }
        return result;
    }

    inline long hashPositionForProbeHashChannelsPage(int position, shared_ptr<DataPage> hashChannelsPage)
    {

        HashCommon hashCommon;
        long result = 0;
        for (int i = 0; i < hashChannelsPage->get()->num_columns(); i++) {
            shared_ptr<arrow::DataType> type = hashChannelsPage->get()->column(i)->type();
            int re = TypeUtils::hashPosition(type,hashChannelsPage->get()->column(i),position);
            result = hashCommon.combineHash(result,re);
        }
        return result;
    }


};





#endif //OLVP_SIMPLEPAGEHASHGENERATOR_HPP
