//
// Created by zxk on 10/18/24.
//

#ifndef FRONTEND_BIGINTOPERATORS_HPP
#define FRONTEND_BIGINTOPERATORS_HPP

#include "SqlOperator.hpp"
#include "../TypeSignature.hpp"
#include <list>
#include <memory>
using namespace std;

class BigInt_Add : public SqlOperator
{

public:
    BigInt_Add() : SqlOperator("BigInt_Add","ADD",StandardTypes::BIGINT,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}

};
class BigInt_Subtract : public SqlOperator
{public:
    BigInt_Subtract() : SqlOperator("BigInt_Subtract","SUBTRACT",StandardTypes::BIGINT,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};

class BigInt_Divide : public SqlOperator
{public:
    BigInt_Divide() : SqlOperator("BigInt_Divide","DIVIDE",StandardTypes::BIGINT,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};
class BigInt_Multiply : public SqlOperator
{public:
    BigInt_Multiply() : SqlOperator("BigInt_Multiply","MULTIPLY",StandardTypes::BIGINT,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};
class BigInt_Modules : public SqlOperator
{public:
    BigInt_Modules() : SqlOperator("BigInt_Modules","MODULES",StandardTypes::BIGINT,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};
class BigInt_GreaterThan : public SqlOperator
{public:
    BigInt_GreaterThan() : SqlOperator("BigInt_GreaterThan","GREATER_THAN",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};
class BigInt_GreaterThanOrEqual : public SqlOperator
{public:
    BigInt_GreaterThanOrEqual() : SqlOperator("BigInt_GreaterThanOrEqual","GREATER_THAN_OR_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};

class BigInt_LessThan : public SqlOperator
{public:
    BigInt_LessThan() : SqlOperator("BigInt_LessThan","LESS_THAN",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};

class BigInt_LessThanOrEqual : public SqlOperator
{public:
    BigInt_LessThanOrEqual() : SqlOperator("BigInt_LessThanOrEqual","LESS_THAN_OR_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};

class BigInt_Equal : public SqlOperator
{public:
    BigInt_Equal() : SqlOperator("BigInt_Equal","EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};

class BigInt_NotEqual : public SqlOperator
{public:
    BigInt_NotEqual() : SqlOperator("BigInt_NotEqual","NOT_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};
class BigInt_And : public SqlOperator
{public:
    BigInt_And() : SqlOperator("BigInt_And","AND",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};
class BigInt_Or : public SqlOperator
{public:
    BigInt_Or() : SqlOperator("BigInt_Or","OR",StandardTypes::BOOLEAN,{StandardTypes::BIGINT,StandardTypes::BIGINT}) {}
};

class BigIntCastToDouble : public SqlOperator
{public:
    BigIntCastToDouble() : SqlOperator("BigIntCastToDouble","CAST",StandardTypes::DOUBLE,{StandardTypes::BIGINT}) {}
};


#endif //FRONTEND_BIGINTOPERATORS_HPP
