//
// Created by zxk on 10/24/24.
//

#ifndef FRONTEND_TPCHCOLUMNHANDLE_HPP
#define FRONTEND_TPCHCOLUMNHANDLE_HPP
#include "ColumnHandle.hpp"
#include "../Analyzer/Type.hpp"
#include <memory>
class TpchColumnHandle : public ColumnHandle
{
    string columnName;
    shared_ptr<Type> type;
public:

    TpchColumnHandle(string columnName,shared_ptr<Type> type): ColumnHandle("TpchColumnHandle"){
        this->columnName = columnName;
        this->type = type;
    }
    string getColumnName(){return this->columnName;}
    shared_ptr<Type> getType(){return this->type;}

};

#endif //FRONTEND_TPCHCOLUMNHANDLE_HPP
