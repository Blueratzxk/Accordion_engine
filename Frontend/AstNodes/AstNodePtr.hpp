//
// Created by zxk on 5/29/23.
//

#ifndef OLVP_ASTNODEPTR_HPP
#define OLVP_ASTNODEPTR_HPP

#include "Node.h"
#include "AstTreeCleaner.hpp"
class AstNodePtr
{
    Node *root;
    std::shared_ptr<AstTreeCleaner> cleaner = std::make_shared<AstTreeCleaner>();


public:

    AstNodePtr()
    {
        this->root = NULL;
    }

    AstNodePtr(Node *root)
    {
        this->root = root;
    }

    Node* get()
    {
        return this->root;
    }

    ~AstNodePtr()
    {
        if(this->root != NULL)
            this->cleaner->Visit(this->root,NULL);
    }

};


#endif //OLVP_ASTNODEPTR_HPP
