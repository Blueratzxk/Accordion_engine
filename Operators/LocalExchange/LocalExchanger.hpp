//
// Created by zxk on 5/27/23.
//

#ifndef OLVP_LOCALEXCHANGER_HPP
#define OLVP_LOCALEXCHANGER_HPP

#include "../../Page/DataPage.hpp"
#include "LocalExchangeSource.hpp"

class LocalExchanger
{
public:

    virtual void accept(std::shared_ptr<DataPage> page) = 0;
    virtual void declareSinkCount() = 0;
    virtual void addSource(std::shared_ptr<LocalExchangeSource> source) = 0;
    virtual bool canExchange() {return true;}
    virtual void closeSource(int number) = 0;
    virtual bool isFinished() = 0;

};


#endif //OLVP_LOCALEXCHANGER_HPP
