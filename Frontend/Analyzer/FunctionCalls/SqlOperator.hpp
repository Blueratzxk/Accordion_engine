//
// Created by zxk on 10/18/24.
//

#ifndef FRONTEND_SQLOPERATOR_HPP
#define FRONTEND_SQLOPERATOR_HPP

#include "../TypeSignature.hpp"
#include <string>
#include <list>
#include <vector>
#include <memory>
using namespace std;
class SqlOperator
{
    std::string name;
    std::string returnType;
    std::string operatorType;
    std::list<std::string> argumentTypes;
public:
    SqlOperator(std::string name, std::string operatorTYpe, std::string returnType,std::list<std::string> argumentTypes) {
        this->name = name;
        this->returnType = returnType;
        this->argumentTypes = argumentTypes;
        this->operatorType = operatorTYpe;
    }

    vector<shared_ptr<TypeSignature>> argumentsToTypeSignatures(){

        vector<shared_ptr<TypeSignature>> types;
        for(auto argument : this->argumentTypes)
        {
            types.push_back(make_shared<TypeSignature>(argument));
        }
        return types;
    }
    std::string  getName(){
        return name;
    }

    std::string getReturnType()
    {
        return this->returnType;
    }

    std::string getOperatorType()
    {
        return this->operatorType;
    }
    vector<std::string> getArgumentNames()
    {
        return {};
    }
    shared_ptr<TypeSignature> getReturnTypeSignature()
    {
        return make_shared<TypeSignature>(this->returnType);
    }
    std::list<std::string> getArgumentTypes()
    {
        return this->argumentTypes;
    }
};
#endif //FRONTEND_SQLOPERATOR_HPP
