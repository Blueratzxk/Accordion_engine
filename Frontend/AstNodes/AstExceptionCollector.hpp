//
// Created by zxk on 10/15/24.
//

#ifndef FRONTEND_ASTEXCEPTIONCOLLECTOR_HPP
#define FRONTEND_ASTEXCEPTIONCOLLECTOR_HPP

#include <iostream>
#include <list>
using namespace std;

class AstExceptionCollector
{
    list<string> errors;
    list<string> infos;
public:
    AstExceptionCollector()
    = default;

    void recordError(string error)
    {
        errors.push_back(error);
    }

    list<string> getErrors()
    {
        return this->errors;
    }



    void recordInfo(string info)
    {
        infos.push_back(info);
    }

    list<string> getInfos()
    {
        return this->infos;
    }

};




#endif //FRONTEND_ASTEXCEPTIONCOLLECTOR_HPP
