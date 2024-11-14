//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_OFFSET_H
#define FRONTEND_OFFSET_H

#include "Node.h"

class Offset : public Node
{
public:
    Offset(string location):Node(location,"Offset"){

    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitOffset(this,context);
    }
};

#endif //FRONTEND_OFFSET_H
