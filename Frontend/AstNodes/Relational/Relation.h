//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_RELATION_H
#define FRONTEND_RELATION_H

#include "../Node.h"
#include "list"
class Relation : public Node
{
    string relationId;
public:
    Relation(string location,string relationId):Node(location,"Relation"){
        this->relationId = relationId;
    }
    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitRelation(this,context);
    }

    virtual ~Relation()= default;
};


#endif //FRONTEND_RELATION_H
