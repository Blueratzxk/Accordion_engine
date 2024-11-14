//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_ORDERBY_H
#define FRONTEND_ORDERBY_H

#include "SortItem.h"
class OrderBy : public Node
{
    list<SortItem*> sortItems;
public:

    OrderBy(string location, list<SortItem*> sortItems) : Node(location,"OrderBy"){
        this->sortItems = sortItems;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitOrderBy(this,context);
    }
    vector<Node *> getChildren() override
    {
        vector<Node *> results;
        for(auto item : sortItems)
        {
            results.push_back(item);
        }
        return results;
    }

};
#endif //FRONTEND_ORDERBY_H
