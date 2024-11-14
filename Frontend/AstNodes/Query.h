//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_QUERY_H
#define FRONTEND_QUERY_H


#include "Relational/QueryBody.h"
class Query : public Relation
{
    QueryBody *queryBody = NULL;
public:
    Query(string location, QueryBody *queryBody) : Relation(location,"Query"){
        this->queryBody = queryBody;
    }
    vector<Node *> getChildren() override
    {
       return {queryBody};
    }

    QueryBody *getQueryBody()
    {
        return this->queryBody;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitQuery(this,context);
    }

};


#endif //FRONTEND_QUERY_H
