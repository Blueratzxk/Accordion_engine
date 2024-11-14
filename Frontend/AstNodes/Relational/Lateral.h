//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_LATERAL_H
#define FRONTEND_LATERAL_H
#include "Relation.h"
#include "../Query.h"
class Lateral : public Relation
{
    Query *query;
public:
    Lateral(string location, Query *query) : Relation(location,"Lateral")
    {
        this->query = query;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitLateral(this,context);
    }


    vector<Node *> getChildren() override
    {
        return {query};
    }
    ~Lateral() override = default;
};


#endif //FRONTEND_LATERAL_H
