//
// Created by zxk on 5/17/23.
//

#ifndef OLVP_CONNECTORTABLEHANDLE_HPP
#define OLVP_CONNECTORTABLEHANDLE_HPP

#include <string>

class ConnectorTableHandle
{
    std::string TableHandleId;
public:


    ConnectorTableHandle(std::string TableHandleId){this->TableHandleId = TableHandleId;}
    std::string getId(){return this->TableHandleId;}

};


#endif //OLVP_CONNECTORTABLEHANDLE_HPP
