//
// Created by zxk on 6/1/23.
//

#ifndef OLVP_PIPELINEID_HPP
#define OLVP_PIPELINEID_HPP

#include <string>

class PipelineId
{
    std::string id = "NULL";

public:
    PipelineId(){}
    PipelineId(std::string id)
    {
        this->id = id;
    }

    std::string get()
    {
       return this->id;
    }

    bool operator<(const PipelineId &p) const //注意这里的两个const
    {
        return id < p.id ;
    }

};
#endif //OLVP_PIPELINEID_HPP
