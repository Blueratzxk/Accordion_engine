//
// Created by zxk on 11/3/24.
//

#ifndef FRONTEND_JOINCRITERIA_HPP
#define FRONTEND_JOINCRITERIA_HPP

#include <string>
class JoinCriteria
{
    std::string joinCriteriaName;
public:
    JoinCriteria(std::string joinCriteriaName)
    {
        this->joinCriteriaName = joinCriteriaName;
    }

    virtual std::string getJoinCriteriaName()
    {
        return this->joinCriteriaName;
    }
    virtual ~JoinCriteria() = default;

};

#endif //FRONTEND_JOINCRITERIA_HPP
