//
// Created by zxk on 11/4/24.
//

#ifndef FRONTEND_FIELDID_HPP
#define FRONTEND_FIELDID_HPP

#include "RelationId.hpp"
#include <memory>
class FieldId
{

    shared_ptr<RelationId> relationId;
    int fieldIndex;
public:


    FieldId(shared_ptr<RelationId> relationId,int fieldIndex){
        this->relationId = relationId;
        this->fieldIndex = fieldIndex;
    }

    shared_ptr<RelationId> getRelationId(){return this->relationId;}
    int getFieldIndex(){return this->fieldIndex;}


    long hashCode()
    {
        return ((long)relationId.get())+fieldIndex;
    }


};


#endif //FRONTEND_FIELDID_HPP
