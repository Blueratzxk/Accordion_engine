//
// Created by zxk on 10/12/24.
//

#ifndef FRONTEND_TYPE_HPP
#define FRONTEND_TYPE_HPP

#include <iostream>
using namespace std;
class Type
{
    string type = "";
public:

    Type(string type)
    {
        this->type = type;
    }
    Type()
    {
    }


    string getType()
    {
        return this->type;
    }


};




#endif //FRONTEND_TYPE_HPP
