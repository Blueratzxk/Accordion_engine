//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_SORTITEM_H
#define FRONTEND_SORTITEM_H

#include "Expression/Expression.h"
class SortItem : public Node
{
public:
    enum Ordering
    {
        ASCENDING, DESCENDING
    };
private:
    Ordering ordering;
    Expression *sortKey;
public:
    SortItem(string location ,Ordering ordering, Expression * sortKey) : Node(location,"SortItem"){
        this->ordering = ordering;
        this->sortKey = sortKey;
    }

    string getOrdering()
    {
        return to_string(this->ordering);
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitSortItem(this,context);
    }

    vector<Node *> getChildren() override
    {
        return {sortKey};
    }



};


#endif //FRONTEND_SORTITEM_H
