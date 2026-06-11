//
// Created by zxk on 5/27/26.
//

#ifndef OLVP_HYPERLOGLOG_HPP
#define OLVP_HYPERLOGLOG_HPP


#include "cpc_sketch.hpp"


class HyperLogLog {

    shared_ptr<datasketches::cpc_sketch> sk;


public:
    HyperLogLog() {

        sk = make_shared<datasketches::cpc_sketch>(12);

    }

    void update(std::shared_ptr<DataPage> page) {


        for (int i = 0 ; i < page->getElementsCount() ; i++)
            sk->update(computeHash(i,page));
    }
    long getEstimation() {
        return sk->get_estimate();
    }

    long computeHash(int position, std::shared_ptr<DataPage> hashChannelsPage)
    {
        HashCommon hashCommon;
        long result = 0;
        for (int i = 0; i < hashChannelsPage->get()->num_columns(); i++) {
            std::shared_ptr<arrow::DataType> type = hashChannelsPage->get()->column(i)->type();
            int re = TypeUtils::hashPosition(type,hashChannelsPage->get()->column(i),position);
            result = hashCommon.combineHash(result,re);
        }
        return result;
    }


};







#endif //OLVP_HYPERLOGLOG_HPP
