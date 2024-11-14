//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_QUERYBODY_H
#define FRONTEND_QUERYBODY_H
#include "Relation.h"

class QueryBody: public Relation
{
    string queryBodyId;
public:
    QueryBody(string location,string queryBodyId) : Relation(location,"QueryBody")
    {
        this->queryBodyId = queryBodyId;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitQueryBody(this,context);
    }

    virtual ~QueryBody()= default;

};

#endif //FRONTEND_QUERYBODY_H
