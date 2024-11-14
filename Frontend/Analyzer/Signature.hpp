//
// Created by zxk on 10/20/24.
//

#ifndef FRONTEND_SIGNATURE_HPP
#define FRONTEND_SIGNATURE_HPP

#include "FunctionCalls/FunctionHandle.hpp"
#include "TypeSignature.hpp"
#include <list>
using  namespace std;

class Signature {
    string name;
    FunctionHandle::type FunctionKind;
    //List<TypeVariableConstraint> typeVariableConstraints;
    //List<LongVariableConstraint> longVariableConstraints;
    shared_ptr<TypeSignature> returnType;
    list<shared_ptr<TypeSignature>> argumentTypes;
    //boolean variableArity;

public:
    Signature(string name,FunctionHandle::type kind,shared_ptr<TypeSignature> returnType,list<shared_ptr<TypeSignature>> argumentTypes){

        this->name = name;
        this->returnType = returnType;
        this->argumentTypes = argumentTypes;
        this->FunctionKind = kind;
    }


    string getName(){return this->name;}
    FunctionHandle::type getFunctionKind(){return this->FunctionKind;}

    shared_ptr<TypeSignature> getReturnType(){return this->returnType;}

    vector<shared_ptr<TypeSignature>> getArgumentTypes(){

        vector<shared_ptr<TypeSignature>> types;
        for(auto argument : this->argumentTypes)
        {
            types.push_back(make_shared<TypeSignature>(argument->getBaseType()));
        }
        return types;
    }

    vector<string> getArgumentNames(){

        return {};
    }

};

#endif //FRONTEND_SIGNATURE_HPP
