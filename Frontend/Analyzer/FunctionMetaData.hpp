//
// Created by zxk on 10/12/24.
//

#ifndef FRONTEND_FUNCTIONMETADATA_HPP
#define FRONTEND_FUNCTIONMETADATA_HPP

#include "Type.hpp"
#include <vector>
#include "TypeSignature.hpp"
#include "FunctionCalls/FunctionKind.hpp"
class FunctionMetadata {
    string name;
    string operatorType;
    vector<shared_ptr<TypeSignature>> argumentTypes;
    vector<string> argumentNames;
    shared_ptr<TypeSignature> returnType;

    FunctionKind functionKind;
    //Optional<Language> language;
    //FunctionImplementationType implementationType;
    //bool deterministic;
    //bool calledOnNullInput;
    //FunctionVersion version;
    //ComplexTypeFunctionDescriptor descriptor;
public:
    FunctionMetadata(string name,FunctionKind functionKind,string operatorType, vector<shared_ptr<TypeSignature>> argumentTypes,vector<string> argumentNames,shared_ptr<TypeSignature> returnType) {

        this->name = name;
        this->operatorType = operatorType;
        this->argumentNames = argumentNames;
        this->argumentTypes = argumentTypes;
        this->returnType = returnType;
        this->functionKind = functionKind;
    }

    string getFunctionKind()
    {
        return this->functionKind.getKind();
    }
    string getOperatorType()
    {
        return this->operatorType;
    }
    vector<shared_ptr<TypeSignature>> getArgumentTypes()
    {
        return this->argumentTypes;
    }
    shared_ptr<TypeSignature> getReturnType()
    {
        return this->returnType;
    }

    string getName()
    {
        return this->name;
    }

    void setArgumentType(int index, shared_ptr<TypeSignature> newArgumentType)
    {
        this->argumentTypes[index] = newArgumentType;
    }
};


#endif //FRONTEND_FUNCTIONMETADATA_HPP
