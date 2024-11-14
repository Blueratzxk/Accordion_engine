//
// Created by zxk on 11/2/24.
//

#ifndef FRONTEND_ROWEXPRESSIONNODEALLOCATOR_HPP
#define FRONTEND_ROWEXPRESSIONNODEALLOCATOR_HPP

#include "RowExpressionVisitor.hpp"
#include "RowExpression.hpp"
#include "CallExpression.hpp"
#include "VariableReferenceExpression.hpp"
#include "ConstantExpression.hpp"
#include "InputReferenceExpression.hpp"



#include <set>
class RowExpressionNodeAllocator
{

    set<RowExpression*> nodes;
public:
    RowExpressionNodeAllocator(){

    }

    CallExpression *new_CallExpression(string sourceLocation,string displayName,
            shared_ptr<FunctionHandle> functionHandle,shared_ptr<Type> returnType,
            list<shared_ptr<RowExpression>> arguments)
    {
        auto node = new CallExpression(sourceLocation,displayName,functionHandle,returnType,arguments);

        nodes.insert(node);
        return node;
    }


    InputReferenceExpression *new_InputReferenceExpression(string location,int field, string  type){

        auto node = new InputReferenceExpression(location,field,type);

        nodes.insert(node);
        return node;
    }


    VariableReferenceExpression *new_VariableReferenceExpression(string location, string name,string type){

        auto node = new VariableReferenceExpression(location,name,type);

        nodes.insert(node);
        return node;

    }

    ConstantExpression *new_ConstantExpression(string location, string value,  string type){

        auto node = new ConstantExpression(location,value,type);

        nodes.insert(node);
        return node;
    }

    void release_all()
    {
        for(auto node : this->nodes)
            delete node;
    }



};


#endif //FRONTEND_ROWEXPRESSIONNODEALLOCATOR_HPP
