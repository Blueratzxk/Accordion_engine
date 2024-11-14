//
// Created by zxk on 11/2/24.
//

#ifndef FRONTEND_BOOLEANOPERATORS_HPP
#define FRONTEND_BOOLEANOPERATORS_HPP



#include "SqlOperator.hpp"
#include "../TypeSignature.hpp"
#include <list>
#include <memory>
using namespace std;

class Boolean_Add : public SqlOperator
{

public:
    Boolean_Add() : SqlOperator("Boolean_Add","ADD",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}

};
class Boolean_Subtract : public SqlOperator
{public:
    Boolean_Subtract() : SqlOperator("Boolean_Subtract","SUBTRACT",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};

class Boolean_Divide : public SqlOperator
{public:
    Boolean_Divide() : SqlOperator("Boolean_Divide","DIVIDE",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};
class Boolean_Multiply : public SqlOperator
{public:
    Boolean_Multiply() : SqlOperator("Boolean_Multiply","MULTIPLY",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};
class Boolean_Modules : public SqlOperator
{public:
    Boolean_Modules() : SqlOperator("Boolean_Modules","MODULES",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};
class Boolean_GreaterThan : public SqlOperator
{public:
    Boolean_GreaterThan() : SqlOperator("Boolean_GreaterThan","GREATER_THAN",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};
class Boolean_GreaterThanOrEqual : public SqlOperator
{public:
    Boolean_GreaterThanOrEqual() : SqlOperator("Boolean_GreaterThanOrEqual","GREATER_THAN_OR_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};

class Boolean_LessThan : public SqlOperator
{public:
    Boolean_LessThan() : SqlOperator("Boolean_LessThan","LESS_THAN",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};

class Boolean_LessThanOrEqual : public SqlOperator
{public:
    Boolean_LessThanOrEqual() : SqlOperator("Boolean_LessThanOrEqual","LESS_THAN_OR_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};

class Boolean_Equal : public SqlOperator
{public:
    Boolean_Equal() : SqlOperator("Boolean_Equal","EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};

class Boolean_NotEqual : public SqlOperator
{public:
    Boolean_NotEqual() : SqlOperator("Boolean_NotEqual","NOT_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};
class Boolean_And : public SqlOperator
{public:
    Boolean_And() : SqlOperator("Boolean_And","AND",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};
class Boolean_Or : public SqlOperator
{public:
    Boolean_Or() : SqlOperator("Boolean_Or","OR",StandardTypes::BOOLEAN,{StandardTypes::BOOLEAN,StandardTypes::BOOLEAN}) {}
};

class BooleanCastToDouble : public SqlOperator
{public:
    BooleanCastToDouble() : SqlOperator("BooleanCastToDouble","CAST",StandardTypes::DOUBLE,{StandardTypes::BOOLEAN}) {}
};
class BooleanCastToBigInt : public SqlOperator
{public:
    BooleanCastToBigInt() : SqlOperator("BooleanCastToBigInt","CAST",StandardTypes::BIGINT,{StandardTypes::BOOLEAN}) {}
};

#endif //FRONTEND_BOOLEANOPERATORS_HPP
