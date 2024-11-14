//
// Created by zxk on 10/12/24.
//

#ifndef FRONTEND_FUNCTIONHANDLE_HPP
#define FRONTEND_FUNCTIONHANDLE_HPP

#include <iostream>
#include <memory>
#include "../CatalogSchemaName.hpp"
using namespace std;
class FunctionHandle {

    string name;

public:
    enum type{SCALAR,AGGREGATE,WINDOW};
    //  shared_ptr<CatalogSchemaName> getCatalogSchemaName();
    FunctionHandle(string name) { this->name = name; }

    virtual string getName(){return name;}

    //SCALAR(1),
    //AGGREGATE(2),
    //WINDOW(3);

    virtual FunctionHandle::type getKind(){return SCALAR;}
};
#endif //FRONTEND_FUNCTIONHANDLE_HPP
