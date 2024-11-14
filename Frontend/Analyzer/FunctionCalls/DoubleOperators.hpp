//
// Created by zxk on 10/18/24.
//

#ifndef FRONTEND_DOUBLEOPERATORS_HPP
#define FRONTEND_DOUBLEOPERATORS_HPP

#include "SqlOperator.hpp"
#include "../TypeSignature.hpp"
#include <list>
#include <memory>
using namespace std;

class Double_Add : public SqlOperator
{

public:
    Double_Add() : SqlOperator("Double_Add","ADD",StandardTypes::DOUBLE,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}

};
class Double_Subtract : public SqlOperator
{public:
    Double_Subtract() : SqlOperator("Double_Subtract","SUBTRACT",StandardTypes::DOUBLE,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};

class Double_Divide : public SqlOperator
{public:
    Double_Divide() : SqlOperator("Double_Divide","DIVIDE",StandardTypes::DOUBLE,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_Multiply : public SqlOperator
{public:
    Double_Multiply() : SqlOperator("Double_Multiply","MULTIPLY",StandardTypes::DOUBLE,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_Modules : public SqlOperator
{public:
    Double_Modules() : SqlOperator("Double_Modules","MODULES",StandardTypes::DOUBLE,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_GreaterThan : public SqlOperator
{public:
    Double_GreaterThan() : SqlOperator("Double_GreaterThan","GREATER_THAN",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_GreaterThanOrEqual : public SqlOperator
{public:
    Double_GreaterThanOrEqual() : SqlOperator("Double_GreaterThanOrEqual","GREATER_THAN_OR_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};

class Double_LessThan : public SqlOperator
{public:
    Double_LessThan() : SqlOperator("Double_LessThan","LESS_THAN",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};

class Double_LessThanOrEqual : public SqlOperator
{public:
    Double_LessThanOrEqual() : SqlOperator("Double_LessThanOrEqual","LESS_THAN_OR_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_Equal : public SqlOperator
{public:
    Double_Equal() : SqlOperator("Double_Equal","EQUAL",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_NotEqual : public SqlOperator
{public:
    Double_NotEqual() : SqlOperator("Double_NotEqual","NOT_EQUAL",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_And : public SqlOperator
{public:
    Double_And() : SqlOperator("Double_And","AND",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};
class Double_Or : public SqlOperator
{public:
    Double_Or() : SqlOperator("Double_Or","OR",StandardTypes::BOOLEAN,{StandardTypes::DOUBLE,StandardTypes::DOUBLE}) {}
};

class DoubleCastToBigInt : public SqlOperator
{public:
    DoubleCastToBigInt() : SqlOperator("DoubleCastToBigInt","CAST",StandardTypes::BIGINT,{StandardTypes::DOUBLE}) {}
};

#endif //FRONTEND_DOUBLEOPERATORS_HPP
