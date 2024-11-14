//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PLANNODEPTR_HPP
#define OLVP_PLANNODEPTR_HPP

#include "PlanNode.hpp"
class PlanNodePtr
{
    PlanNode *root = NULL;
    std::shared_ptr<PlanNodeTreeCleaner> cleaner = std::make_shared<PlanNodeTreeCleaner>();

public:

    PlanNodePtr()
    {
        this->root = NULL;
    }

    PlanNodePtr(PlanNode *root)
    {
        this->root = root;
    }

    PlanNode* get()
    {
        return this->root;
    }

    ~PlanNodePtr()
    {
        if(this->root != NULL)
            this->cleaner->Visit(this->root,NULL);
    }

};


#endif //OLVP_PLANNODEPTR_HPP
