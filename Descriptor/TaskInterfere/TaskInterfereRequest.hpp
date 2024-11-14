//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_TASKINTERFEREREQUEST_HPP
#define OLVP_TASKINTERFEREREQUEST_HPP

#include <string>
class TaskInterfereRequest
{
    std::string type;


public:
    TaskInterfereRequest(std::string type)
    {
        this->type = type;
    }

    std::string getType() {return this->type;}

};

#endif //OLVP_TASKINTERFEREREQUEST_HPP
