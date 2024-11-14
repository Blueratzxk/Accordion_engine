//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_SELECT_H
#define FRONTEND_SELECT_H

#include "SelectItem.h"
class Select : public Node
{

    bool distinct;
    list<SelectItem *> selectItems;

public:
    Select(string location,list<SelectItem *> selectItems): Node(location,"Select"){
        this->selectItems = selectItems;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitSelect(this,context);
    }

    list<SelectItem *> getSelectItems()
    {
        return this->selectItems;
    }

    vector<Node *> getChildren() override
    {
        vector<Node*> results;

        for(auto item : selectItems)
            results.push_back(item);

        return results;
    }
};



#endif //FRONTEND_SELECT_H
