//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_ISDISTINCT_H
#define FRONTEND_ISDISTINCT_H
#include "Node.h"
class IsDistinct : public Node
{

public:
    IsDistinct(string location) : Node(location,"IsDistinct"){}

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitIsDistinct(this,context);
    }
};

#endif //FRONTEND_ISDISTINCT_H
