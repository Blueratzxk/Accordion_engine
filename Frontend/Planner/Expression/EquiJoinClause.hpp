//
// Created by zxk on 11/3/24.
//

#ifndef FRONTEND_EQUIJOINCLAUSE_HPP
#define FRONTEND_EQUIJOINCLAUSE_HPP

#include "VariableReferenceExpression.hpp"

class EquiJoinClause
{
    shared_ptr<VariableReferenceExpression> left;
    shared_ptr<VariableReferenceExpression> right;

public:
    EquiJoinClause(shared_ptr<VariableReferenceExpression> left,shared_ptr<VariableReferenceExpression> right)
    {
        this->left = left;
        this->right = right;
    }

    shared_ptr<VariableReferenceExpression> getLeft()
    {
        return this->left;
    }
    shared_ptr<VariableReferenceExpression> getRight()
    {
        return this->right;
    }

};



#endif //FRONTEND_EQUIJOINCLAUSE_HPP
