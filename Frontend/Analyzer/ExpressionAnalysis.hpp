//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_EXPRESSIONANALYSIS_HPP
#define FRONTEND_EXPRESSIONANALYSIS_HPP


#include "Type.hpp"
class TypeAllocator;

class FunctionHandle;
class FieldId;
class ExpressionAnalysis
{
    map<Expression *, Type *> Types;
    map<Expression *, Type *> Coercions;
    map<Expression *, string> functionCalls;
    map<string, shared_ptr<FunctionHandle>> functionHandles;
    shared_ptr<TypeAllocator> typeAllocator;
    set<Expression *> operators;
    map<Expression *,shared_ptr<FieldId>> columnReferences;
public:
    ExpressionAnalysis(map<Expression *, Type *> Types, map<Expression *, string> functionCalls,
                       map<string, shared_ptr<FunctionHandle>> functionHandles,map<Expression *, Type *> Coercions,
                       shared_ptr<TypeAllocator> typeAllocator,set<Expression *> operators,
                       map<Expression *,shared_ptr<FieldId>> columnReferences)
    {
        this->Types = Types;
        this->functionCalls = functionCalls;
        this->functionHandles = functionHandles;
        this->typeAllocator = typeAllocator;
        this->Coercions = Coercions;
        this->operators = operators;
        this->columnReferences = columnReferences;
    }
    map<Expression *, Type *> getTypes(){return this->Types;}
    map<Expression *, string> getFunctionCalls(){return this->functionCalls;}
    map<string, shared_ptr<FunctionHandle>> getFunctionHandles(){return this->functionHandles;}
    map<Expression *, Type *> getCoercions(){return this->Coercions;}
    shared_ptr<TypeAllocator> getTypeAllocator(){return this->typeAllocator;}
    set<Expression *> getOperators(){return this->operators;}
    map<Expression *,shared_ptr<FieldId>> getColumnReferences(){return this->columnReferences;}


};


#endif //FRONTEND_EXPRESSIONANALYSIS_HPP
