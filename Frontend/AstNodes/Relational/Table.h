//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_TABLE_H
#define FRONTEND_TABLE_H

#include "QueryBody.h"

class Table : public QueryBody
{
    string tableName;

public:
    Table(string location, string tableName) : QueryBody(location,"Table"){
        this->tableName = tableName;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitTable(this,context);
    }

    string getTableName()
    {
        return this->tableName;
    }


};


#endif //FRONTEND_TABLE_H
