//
// Created by zxk on 10/24/24.
//

#ifndef FRONTEND_PLANNODEIDALLOCATOR_HPP
#define FRONTEND_PLANNODEIDALLOCATOR_HPP
#include <string>
class PlanNodeIdAllocator
{
    int nextId = 0;
public:
    PlanNodeIdAllocator(){}

    std::string getNextId()
    {
        return std::to_string(nextId++);
    }
};
#endif //FRONTEND_PLANNODEIDALLOCATOR_HPP
