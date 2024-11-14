//
// Created by zxk on 10/30/24.
//

#ifndef FRONTEND_FUNCTIONKIND_HPP
#define FRONTEND_FUNCTIONKIND_HPP

#include <string>
class FunctionKind
{
    int kind;
    vector<std::string> type{"SCALAR","AGGREGATE","WINDOW"};
public:
    FunctionKind(int kind){
        this->kind = kind;
    }

    FunctionKind(){
        this->kind = 0;
    }

    std::string getKind()
    {
       return type[kind];
    }

};

#endif //FRONTEND_FUNCTIONKIND_HPP
