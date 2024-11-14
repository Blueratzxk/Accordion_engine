//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_JOIN_H
#define FRONTEND_JOIN_H

#include "Relation.h"
#include "../JoinCriteria/JoinCriteria.hpp"
#include <memory>
class Join : public Relation
{

public:
    enum Type
    {
        CROSS, INNER, LEFT, RIGHT, FULL, IMPLICIT
    };
private:
    Type type;
    Relation *left = NULL;
    Relation *right = NULL;
    shared_ptr<JoinCriteria>  predicate = NULL;

public:
    Join(string location, Type type,Relation *left, Relation *right) : Relation(location,"Join")
    {
        this->type = type;
        this->left = left;
        this->right = right;
    }
    Join(string location, Type type,Relation *left, Relation *right, shared_ptr<JoinCriteria> predicate) : Relation(location,"Join")
    {
        this->type = type;
        this->left = left;
        this->right = right;
        this->predicate = predicate;
    }

    void* accept(AstNodeVisitor* visitor,void* context)  {
        return visitor->VisitJoin(this,context);
    }

    shared_ptr<JoinCriteria> getJoinCriteria(){
        return this->predicate;
    }

    Relation *getLeft()
    {
        return this->left;
    }

    Relation *getRight()
    {
        return this->right;
    }


    string getType()
    {
        return to_string(this->type);
    }

    vector<Node *> getChildren() override
    {
        return {left,right};
    }


};
#endif //FRONTEND_JOIN_H
