//
// Created by zxk on 10/4/24.
//

#ifndef FRONTEND_FIELDREFERENCE_HPP
#define FRONTEND_FIELDREFERENCE_HPP

#include "../AstNodes/Expression/Expression.h"

class FieldReference : public Expression
{
    string location;
    int fieldIndex;
public:
    FieldReference(string location, int fieldIndex) : Expression(location,"FieldReference")
    {
        this->fieldIndex = fieldIndex;
        this->location = location;
    }


};
#endif //FRONTEND_FIELDREFERENCE_HPP
