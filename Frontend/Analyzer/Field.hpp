//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_FIELD_HPP
#define FRONTEND_FIELD_HPP
#include <iostream>
#include <string>
#include <list>
#include <memory>
#include <map>
using namespace std;
class Field
{
    string value;
    string type;
    string location;

public:
    Field(string location,string value,string type)
    {
        this->value = value;
        this->type = type;
        this->location = location;
    }

    string getValue()
    {
        return this->value;
    }
    string getType()
    {
        return this->type;
    }


    string getNodeLocation()
    {
        return this->location;
    }


};


#endif //FRONTEND_FIELD_HPP
