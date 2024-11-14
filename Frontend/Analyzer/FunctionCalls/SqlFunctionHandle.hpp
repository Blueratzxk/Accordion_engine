//
// Created by zxk on 10/18/24.
//

#ifndef FRONTEND_SQLFUNCTIONHANDLE_HPP
#define FRONTEND_SQLFUNCTIONHANDLE_HPP

#include "FunctionHandle.hpp"
class SqlFunctionHandle : public FunctionHandle
{
    string functionId;
public:
    SqlFunctionHandle(string functionId) : FunctionHandle("SqlFunctionHandle") {
        this->functionId = functionId;
    }

    FunctionHandle::type getKind() override
    {
        return FunctionHandle::SCALAR;
    }

    string getFunctionId()
    {
        return this->functionId;
    }


};
#endif //FRONTEND_SQLFUNCTIONHANDLE_HPP
