//
// Created by zxk on 10/22/24.
//

#ifndef FRONTEND_FIELDREFERENCE_HPP
#define FRONTEND_FIELDREFERENCE_HPP

#include "Expression.h"

class FieldReference : public Expression
{
    int fieldIndex;
public:
    FieldReference(string location, int fieldIndex): Expression(location,"FieldReference")
    {
        this->fieldIndex = fieldIndex;
    }
    int getFieldIndex()
    {
        return fieldIndex;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitFieldReference(this,context);
    }


};

#endif //FRONTEND_FIELDREFERENCE_HPP
