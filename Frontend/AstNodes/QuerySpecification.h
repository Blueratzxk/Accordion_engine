//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_QUERYSPECIFICATION_H
#define FRONTEND_QUERYSPECIFICATION_H

#include "Relational/QueryBody.h"
#include "Expression/Expression.h"
#include "GroupBy.h"
#include "OrderBy.h"
#include "Offset.h"

class QuerySpecification : public QueryBody
{
    Select *select = NULL;
    Relation *from = NULL;
    Expression *where = NULL;
    GroupBy *groupBy = NULL;
    Expression *having = NULL;
    OrderBy *orderBy = NULL;
    Offset *offset = NULL;
    string limit;
public:
    QuerySpecification(string location, Select *select, Relation *from,Expression *where,GroupBy *groupBy,Expression *having,OrderBy *orderBy,Offset *offset,string limit)
    : QueryBody(location,"QuerySepecification")
    {
        this->select = select;
        this->from = from;
        this->where = where;
        this->groupBy = groupBy;
        this->having = having;
        this->orderBy = orderBy;
        this->offset = offset;
        this->limit = limit;
    }


    Node *getFrom()
    {
        return this->from;
    }
    Expression *getWhere()
    {
        return this->where;
    }

    Select *getSelect()
    {
        return this->select;
    }

    GroupBy* getGroupBy()
    {
        return this->groupBy;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitQuerySpecification(this,context);
    }

    vector<Node *> getChildren() override
    {
        list<Node*> result = {select,from,where,groupBy,having,orderBy,offset};
        vector<Node*> out;

        for(auto re : result)
        {
            if(re != NULL)
                out.push_back(re);
        }

        return out;
    }
};

#endif //FRONTEND_QUERYSPECIFICATION_H
