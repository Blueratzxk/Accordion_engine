//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_RELATIONID_HPP
#define FRONTEND_RELATIONID_HPP


#include "../AstNodes/Node.h"

class RelationId
{
    Node *sourceNode;
public:
    RelationId(Node *node)
    {
        this->sourceNode = node;
    }

    Node *getRelationId()
    {
        return this->sourceNode;
    }


};


#endif //FRONTEND_RELATIONID_HPP
