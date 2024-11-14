//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_ALLCOLUMNS_H
#define FRONTEND_ALLCOLUMNS_H

#include "SelectItem.h"

class AllColumns : public SelectItem
{
public:
    AllColumns(string location) : SelectItem(location,"AllColumns"){

    }
    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitAllColumns(this,context);
    }

};



#endif //FRONTEND_ALLCOLUMNS_H
