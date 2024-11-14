//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_ANALYSIS_HPP
#define FRONTEND_ANALYSIS_HPP

#include "../AstNodes/tree.h"
#include <map>
#include <unordered_map>
#include "memory"
#include "Scope.hpp"
#include "Type.hpp"
#include "TypeAllocator.hpp"
#include "../Planner/TpchColumnHandle.hpp"
#include "../Planner/TableHandle.hpp"
#include "FunctionCalls/FunctionHandle.hpp"
#include "FieldId.hpp"

class Analysis {

    string statement;


    map<Node *, Scope*> scopes;
    list<Scope *> voidScopes;



    map<QuerySpecification*, list<FunctionCall *>> aggregates;

    map<string, shared_ptr<FunctionHandle>> aggregateFunctionHandles;
    map<OrderBy*, list<Expression *>> orderByAggregates;
    map<QuerySpecification*, list<Expression*>> groupByExpressions;

    map<Node *, Expression*> where;
    map<QuerySpecification*, Expression*> having;
    map<Node *, list<Expression*>> orderByExpressions;

    map<Node *, list<Expression*>> outputExpressions;


    map<Join *, Expression *> joins;

    map<shared_ptr<Field>, shared_ptr<ColumnHandle>> columns;
    map<Table*, shared_ptr<TableHandle>> tables;

    map<Expression *, Type *> Types;
    map<Expression *, Type *> Coercion;
    map<Expression *,shared_ptr<FieldId>> columnReferences;


//MetadataHandle metadataHandle = new MetadataHandle();
//final Map<NodeRef<Table>, TableHandle> tables = new LinkedHashMap<>();

    map<Expression *, string> types;


    set<shared_ptr<TypeAllocator>> typeAllocators;
//    map<Field, ColumnHandle> columns = new LinkedHashMap<>();


public:
    Analysis(string statement)
    {
        this->statement = statement;
    }
    Type *getCoercion(Expression *node)
    {
        if(this->Coercion.contains(node))
            return this->Coercion[node];
        else
            return NULL;
    }
    Type *getTypeWithCoercions(Expression *node)
    {
        if(this->Coercion.contains(node))
            return this->Coercion[node];
        else
            return this->Types[node];
    }


    shared_ptr<FunctionHandle> getFunctionHandle(FunctionCall *functionCall)
    {
        for(auto function : this->aggregateFunctionHandles)
        {
            if(function.first == functionCall->getFuncName())
                return function.second;
        }


        return NULL;
    }

    void addTypeAllocator(shared_ptr<TypeAllocator> allocator)
    {
        this->typeAllocators.insert(allocator);
    }

    void setAggregateFunctionHandle(string functionName,shared_ptr<FunctionHandle> functionHandle)
    {
        this->aggregateFunctionHandles[functionName] = functionHandle;
    }

    void setScope(Node *node, Scope *scope)
    {
        scopes[node] = scope;
    }

    void setWhere(Node *node, Expression *expression)
    {
        where[node] = expression;
    }

    void setOutputExpressions(Node *node, list<Expression*> expressions)
    {
        outputExpressions[node] = expressions;
    }



    list<Expression*> getOutputExpressions(Node *node)
    {
        return outputExpressions[node];
    }

    void setColumn(shared_ptr<Field> field, shared_ptr<ColumnHandle> handle)
    {
        columns[field] = handle;
    }
    shared_ptr<ColumnHandle> getColumn(shared_ptr<Field> field)
    {
        return columns[field];
    }

    void registerTable(Table *table, shared_ptr<TableHandle> handle)
    {
        tables[table] = handle;
    }
    shared_ptr<TableHandle> getTableHandle(Table *table)
    {
        return tables[table];
    }

    void setAggregates(QuerySpecification *node, list<FunctionCall*> functionCalls)
    {
        this->aggregates[node] = functionCalls;
    }

    list<FunctionCall*> getAggregates(QuerySpecification *node)
    {
        return this->aggregates[node];
    }
    void setGroupByExpressions(QuerySpecification *node, list<Expression*> expressions)
    {
        this->groupByExpressions[node] = expressions;
    }

    list<Expression*> getGroupByExpressions(QuerySpecification *node)
    {
        return this->groupByExpressions[node];
    }
    void registerJoin(Join *join, Expression *expression)
    {
        joins[join] = expression;
    }

    bool hasAggregation(QuerySpecification *node)
    {
        if(this->groupByExpressions.count(node) > 0)
            return true;
        else
            return false;
    }

    void setColumnReference(Expression *node,shared_ptr<FieldId> fieldId)
    {
        if(!this->columnReferences.contains(node))
            this->columnReferences[node] = fieldId;
    }

    map<Expression *,shared_ptr<FieldId>> getColumnReferences(){
        return this->columnReferences;
    }

    shared_ptr<FieldId> getColumnReference(Expression *node)
    {
        return this->columnReferences[node];
    }


    void setType(Expression *node, Type *type)
    {
     //   if(this->Types.count(node) > 0)
     //   {
     //       delete(this->Types[node]);
     //       this->Types[node] = type;
     //   }
      //  else
            this->Types[node] = type;
    }

    void setCoercion(Expression *node, Type *type)
    {
     //   if(this->Coercion.count(node) > 0)
      //  {
      //      delete(this->Coercion[node]);
       //     this->Coercion[node] = type;
       // }
       // else
            this->Coercion[node] = type;
    }

    Expression *getWhere(Node *node)
    {
        return this->where[node];
    }

    Type *getType(Expression *node)
    {
        if(this->Types.count(node) > 0)
            return this->Types[node];
        else
            return NULL;
    }

    map<Expression *, Type *> getExpressions()
    {
        return this->Types;
    }

    void addVoidScope(Scope *scope)
    {
        this->voidScopes.push_back(scope);
    }

    Scope *getScope(Node *node)
    {
        return this->scopes[node];
    }

    ~Analysis() {

        for (auto scope: scopes) {
            delete (scope.second);
        }

        for (auto typeAlloc: this->typeAllocators) {
            typeAlloc->release_all();
        }

        for (auto voidscope: voidScopes) {
            delete (voidscope);
        }
    }


};


#endif //FRONTEND_ANALYSIS_HPP
