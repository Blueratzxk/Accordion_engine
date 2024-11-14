//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_SELECTITEM_H
#define FRONTEND_SELECTITEM_H

#include "Node.h"
class SelectItem:public Node
{
    string selectItemId;
public:
    SelectItem(string location,string selectItemId): Node(location,"SelectItem")
    {
        this->selectItemId = selectItemId;
    }

    string getSelectItemId()
    {
        return this->selectItemId;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitSelectItem(this,context);
    }

    ~SelectItem() override = default;

};


#endif //FRONTEND_SELECTITEM_H
