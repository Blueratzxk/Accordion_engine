//
// Created by zxk on 10/15/24.
//

#ifndef FRONTEND_EXCEPTIONCOLLECTOR_HPP
#define FRONTEND_EXCEPTIONCOLLECTOR_HPP

#include <iostream>
#include <list>
using namespace std;

class ExceptionCollector
{
    set<string> errors;
    set<string> warns;
public:
    ExceptionCollector()
    {

    }

    void recordError(string error)
    {
        if(!errors.contains(error))
            errors.insert(error);
    }

    set<string> getErrors()
    {
        return this->errors;
    }

    void recordWarn(string warn)
    {
        if(!warns.contains(warn))
            warns.insert(warn);
    }

    set<string> getWarns()
    {
        return this->warns;
    }

};
#endif //FRONTEND_EXCEPTIONCOLLECTOR_HPP
