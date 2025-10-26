//
// Created by zxk on 10/24/25.
//

#ifndef OLVP_JOINBUILDCOMPONENTS_HPP
#define OLVP_JOINBUILDCOMPONENTS_HPP

#include <string>
class JoinBuildComponents
{
    std::string joinType;
public:
    JoinBuildComponents(std::string joinType){
        this->joinType = joinType;
    }

    std::string  getJoinType()
    {
        return this->joinType;
    }

    virtual void fromDataPages(vector<shared_ptr<DataPage>> pages) = 0;
    virtual vector<shared_ptr<DataPage>> ToDataPages() = 0;
};


#endif //OLVP_JOINBUILDCOMPONENTS_HPP
