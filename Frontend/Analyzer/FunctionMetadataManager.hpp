//
// Created by zxk on 10/12/24.
//

#ifndef FRONTEND_FUNCTIONMETADATAMANAGER_HPP
#define FRONTEND_FUNCTIONMETADATAMANAGER_HPP

#include "FunctionCalls/FunctionHandle.hpp"
#include "FunctionMetaData.hpp"

#include <map>
#include "spdlog/spdlog.h"

#include "BuildInFunctionMetas.hpp"

class FunctionMetadataManager
{

    shared_ptr<BuildInFunctionMetas> buildInFunctionMetas = make_shared<BuildInFunctionMetas>();

public:
    FunctionMetadataManager()
    {

    }


    list<shared_ptr<FunctionHandle>> resolveFunction(string operatorType)
    {
        return this->buildInFunctionMetas->getFunctionsByOperatorType(operatorType);
    }

    list<shared_ptr<FunctionHandle>> resolveFunctionInternal(string name)
    {
       return this->buildInFunctionMetas->getInternalFunctionsByName(name);
    }


    shared_ptr<FunctionMetadata> getFunctionMetadata(shared_ptr<FunctionHandle> functionHandle)
    {
        return this->buildInFunctionMetas->getFunctionMetadata(functionHandle);
    }


    list<shared_ptr<FunctionHandle>> resolveOperatorPermitCoercion(list<shared_ptr<FunctionHandle>> candidates, vector<shared_ptr<TypeSignature>> Types)
    {

        list<shared_ptr<FunctionHandle>> functionCandidates;



        for(auto candidate : candidates) {

            bool candidateOk = true;
           auto arguments = this->getFunctionMetadata(candidate)->getArgumentTypes();

           if(arguments.size() != Types.size())
               continue;

            for(int i = 0 ; i < Types.size() ; i++){
                if(Types[i]->getBaseType() != arguments[i]->getBaseType() && !canCast(Types[i],arguments[i])) {
                    candidateOk = false;
                    break;
                }
            }
            if(candidateOk)
                functionCandidates.push_back(candidate);
        }
        return functionCandidates;
    }

    bool canCast(shared_ptr<TypeSignature> typeOrigin,shared_ptr<TypeSignature> typeDest)
    {

        auto castFunctions = this->resolveFunction("CAST");

        for(auto cast : castFunctions)
        {
            auto castFunction = this->getFunctionMetadata(cast);
            auto castArguments = castFunction->getArgumentTypes();
            auto returnType = castFunction->getReturnType();
            if(castArguments.size() == 1 && castArguments[0]->getBaseType() == typeOrigin->getBaseType())
                if(returnType->getBaseType() == typeDest->getBaseType())
                    return true;
        }

        return false;
    }


};


#endif //FRONTEND_FUNCTIONMETADATAMANAGER_HPP
