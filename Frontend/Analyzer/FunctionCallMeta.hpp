//
// Created by zxk on 10/12/24.
//

#ifndef FRONTEND_FUNCTIONCALLMETA_HPP
#define FRONTEND_FUNCTIONCALLMETA_HPP

#include <iostream>
#include <map>
using namespace std;
class FunctionCallMeta
{

    map<string,string> functionType = {
            {"count","int"},
            {"avg","double"},
    };
public:
    FunctionCallMeta()
    {

    }

    string getFunctionType(string funcName)
    {
        return functionType[funcName];
    }

};


#endif //FRONTEND_FUNCTIONCALLMETA_HPP
