//
// Created by zxk on 10/31/24.
//

#ifndef FRONTEND_SIMPLEGROUPBY_HPP
#define FRONTEND_SIMPLEGROUPBY_HPP

#include "GroupingElement.h"

class SimpleGroupBy : public GroupingElement
{
    list<Expression *> columns;
public:
    SimpleGroupBy(string location, list<Expression *> columns) : GroupingElement(location,"SimpleGroupBy")
    {
        this->columns = columns;
    }
    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitSimpleGroupBy(this,context);
    }

    list<Expression *> getExpressions()
    {
        return this->columns;
    }


    vector<Node *> getChildren() override
    {
        vector<Node *> childs;

        for(auto col : columns)
        {
            childs.push_back(col);
        }

        return childs;
    }

};


#endif //FRONTEND_SIMPLEGROUPBY_HPP
