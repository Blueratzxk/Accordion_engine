//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_SCOPE_HPP
#define FRONTEND_SCOPE_HPP
#include <iostream>
#include "memory"

using namespace std;

#include "RelationId.hpp"
#include "RelationType.hpp"
#include "spdlog/spdlog.h"


class Scope
{
    string scopeName;
    Scope *parent = NULL;
    shared_ptr<RelationId> relationId;
    shared_ptr<RelationType> relationType;
    bool queryBoundary;

public:


    class ResolvedField
    {
        Scope *scope;
        shared_ptr<Field> field;
        int hierarchyFieldIndex;
        int relationFieldIndex;
        bool local;


    public:

        ResolvedField(Scope *scope, shared_ptr<Field> field, int hierarchyFieldIndex, int relationFieldIndex, bool local)
        {
            this->hierarchyFieldIndex = hierarchyFieldIndex;
            this->relationFieldIndex = relationFieldIndex;
            this->local = local;
            this->scope = scope;
            this->field = field;
        }

        string getType()
        {
            return field->getType();
        }

        Scope *getScope()
        {
            return scope;
        }


        bool isLocal()
        {
            return local;
        }

        int getHierarchyFieldIndex()
        {
            return hierarchyFieldIndex;
        }

        int getRelationFieldIndex()
        {
            return relationFieldIndex;
        }

        shared_ptr<Field> getField()
        {
            return field;
        }
    };

    Scope(string scopeName,Scope *parent,bool queryBoundary,shared_ptr<RelationId> relationId,shared_ptr<RelationType> relationType)
    {
        this->scopeName = scopeName;
        this->parent = parent;
        this->queryBoundary = queryBoundary;
        this->relationType = relationType;
        this->relationId = relationId;
    }

    Scope(string scopeName,Node *node,bool queryBoundary)
    {
        this->scopeName = scopeName;
        this->parent = NULL;
        this->queryBoundary = queryBoundary;
        this->relationType = make_shared<RelationType>();
        this->relationId = make_shared<RelationId>(node);
    }

    shared_ptr<RelationType> getRelationType()
    {
        return this->relationType;
    }

    shared_ptr<ResolvedField> asResolvedField(shared_ptr<Field> field, int fieldIndexOffset, bool local)
    {
        int relationFieldIndex = relationType->indexOf(field);
        int hierarchyFieldIndex = relationType->indexOf(field) + fieldIndexOffset;
        auto result = make_shared<ResolvedField>(this, field, hierarchyFieldIndex, relationFieldIndex, local);
        return result;
    }

    shared_ptr<ResolvedField> resolveField(Expression *node, string name, int fieldIndexOffset, bool local)
    {
        vector<shared_ptr<Field>> matches = this->relationType->resolveFields(name);
        if (matches.size() > 1) {
            spdlog::error("Ambiguous Attribute!");
            return NULL;
        }
        else if (matches.size() == 1) {

            auto result = asResolvedField(matches[0], fieldIndexOffset, local);
            return result;
        }
        else {
          //  if (isColumnReference(name, relationType)) {
         //       return Optional.empty();
          //  }
            if (parent != NULL) {
                return parent->resolveField(node, name, fieldIndexOffset + relationType->getAllFieldCount(), local && !queryBoundary);
            }
            return NULL;
        }
    }

    shared_ptr<ResolvedField> tryResolveField(Expression *node)
    {
        string ename = "";
        if(node->getExpressionId() == "Identifier")
        {
            ename = ((Identifier*)node)->getValue();
        }

        return resolveField(node, ename, 0, true);
    }


    shared_ptr<RelationId> getRelationId(){
        return this->relationId;
    }

    Scope *getLocalParent()
    {
        if (!queryBoundary) {
            return parent;
        }

        return NULL;
    }

};


class ScopeBuilder : public enable_shared_from_this<ScopeBuilder> {
    shared_ptr<RelationId> relationId;
    shared_ptr<RelationType> relationType;

//Map<String, WithQuery> namedQueries = new HashMap<>();
    Scope *parent = NULL;
    bool queryBoundary = false;

public:
    ScopeBuilder() {
        Node *node = NULL;
        this->relationId = make_shared<RelationId>(node);
        this->relationType = make_shared<RelationType>();
    }

    ScopeBuilder(shared_ptr<RelationId> relationId, shared_ptr<RelationType> relationType) {
        this->relationId = relationId;
        this->relationType = relationType;
    }

    shared_ptr<ScopeBuilder>
    withRelationType(shared_ptr<RelationId> relationId, shared_ptr<RelationType> relationType) {
        this->relationId = relationId;
        this->relationType = relationType;
        return shared_from_this();
    }

    shared_ptr<ScopeBuilder> withParent(Scope *parent) {
        //       checkArgument(!this.parent.isPresent(), "parent is already set");
        this->parent = parent;
        return shared_from_this();
    }

    shared_ptr<ScopeBuilder> withOuterQueryParent(Scope *parent) {
        //  checkArgument(!this.parent.isPresent(), "parent is already set");
        this->parent = parent;
        this->queryBoundary = true;
        return shared_from_this();
    }



//public Builder withNamedQuery(String name, WithQuery withQuery)
//    {
    //       checkArgument(!containsNamedQuery(name), "Query '%s' is already added", name);
    //       namedQueries.put(name, withQuery);
    //       return this;
    //   }

//public boolean containsNamedQuery(String name)
//    {
    //       return namedQueries.containsKey(name);
    //   }

    Scope* build(string name) {
        return new Scope(name,parent, queryBoundary, relationId, relationType);
    }
};


#endif //FRONTEND_SCOPE_HPP
