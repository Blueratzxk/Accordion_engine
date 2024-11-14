//
// Created by zxk on 11/2/24.
//

#ifndef FRONTEND_TYPEALLOCATOR_HPP
#define FRONTEND_TYPEALLOCATOR_HPP

#include "Type.hpp"
#include <set>
class TypeAllocator
{
    set<Type*> types;
public:
    TypeAllocator()
    {

    }

    Type *new_Type(string type)
    {
        auto obj = new Type(type);
        this->types.insert(obj);
        return obj;
    }

    void release_all()
    {
        for(auto type: types)
        {
            delete type;
        }
    }


};


#endif //FRONTEND_TYPEALLOCATOR_HPP
