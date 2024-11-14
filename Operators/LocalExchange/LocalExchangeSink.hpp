//
// Created by zxk on 5/28/23.
//

#ifndef OLVP_LOCALEXCHANGESINK_HPP
#define OLVP_LOCALEXCHANGESINK_HPP

#include "../../common.h"
#include "LocalExchanger.hpp"

class LocalExchangeSink
{
    std::shared_ptr<LocalExchanger> exchanger;



public:

    LocalExchangeSink( std::shared_ptr<LocalExchanger> exchanger)
    {
        this->exchanger = exchanger;
    }

    void addPage(std::shared_ptr<DataPage> page)
    {
        while(!this->exchanger->canExchange());

            this->exchanger->accept(page);
    }

};


#endif //OLVP_LOCALEXCHANGESINK_HPP
