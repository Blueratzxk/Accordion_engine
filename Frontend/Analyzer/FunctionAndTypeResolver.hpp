//
// Created by zxk on 10/12/24.
//

#ifndef FRONTEND_FUNCTIONANDTYPERESOLVER_HPP
#define FRONTEND_FUNCTIONANDTYPERESOLVER_HPP


#include "FunctionCalls/FunctionHandle.hpp"
#include "Type.hpp"
#include <vector>
#include "FunctionMetadataManager.hpp"
#include "ExceptionCollector.hpp"

class FunctionAndTypeResolver : public FunctionMetadataManager
{
    shared_ptr<FunctionMetadataManager> functionMetadataManager;

    map<shared_ptr<FunctionHandle>,shared_ptr<FunctionMetadata>> cachedInternalFunctions;

    shared_ptr<ExceptionCollector> exceptionCollector;

public:
    FunctionAndTypeResolver(shared_ptr<ExceptionCollector> exceptionCollector)
    {
        this->functionMetadataManager = make_shared<FunctionMetadataManager>();
        this->exceptionCollector = exceptionCollector;
    }

    shared_ptr<FunctionMetadataManager> getFunctionMetadataManager()
    {
        return this->functionMetadataManager;
    }

    shared_ptr<FunctionHandle> resolveFunction(string funcName, vector<Type *> Types)
    {
        list<shared_ptr<FunctionHandle>> candidates = this->functionMetadataManager->resolveFunctionInternal(funcName);

        shared_ptr<FunctionHandle> matchedFunctionHandle = NULL;

        vector<shared_ptr<TypeSignature>> expressionArgumentsTypes;
        for(int i = 0 ;i < Types.size() ; i++)
        {
            if(Types[i] != NULL)
                expressionArgumentsTypes.push_back(TypeSignature::parseTypeSignature(Types[i]->getType()));
            else
                return NULL;
        }

        string argumentsString;
        for(int i = 0 ;i < expressionArgumentsTypes.size() ; i++)
            argumentsString+=(expressionArgumentsTypes[i]->getBaseType()+",");
        if(!argumentsString.empty())
            argumentsString.pop_back();

        for(auto handle =  candidates.begin() ;handle != candidates.end(); handle++)
        {
            shared_ptr<FunctionMetadata> functionMetadata;
            functionMetadata = this->functionMetadataManager->getFunctionMetadata(*handle);

            auto functionArgumentTypes = functionMetadata->getArgumentTypes();

            if(functionArgumentTypes.size() != Types.size()) {
          //      this->exceptionCollector->recordError("Function "+funcName+"("+argumentsString+") arugments number error!");
                continue;
            }

            bool matched = true;
            for(int i = 0 ;i < expressionArgumentsTypes.size() ; i++)
            {
                auto functionType = functionArgumentTypes[i]->getBaseType();
                auto eType = expressionArgumentsTypes[i]->getBaseType();

                if(functionType != eType){
                    matched = false;

                    break;
                }
            }

            if(matched == true){
                matchedFunctionHandle = *handle;
                break;
            }

        }

        if(matchedFunctionHandle == NULL) {


            auto coerce = this->functionMetadataManager->resolveOperatorPermitCoercion(candidates,expressionArgumentsTypes);
            if(!coerce.empty())
            {

                auto select = coerce.front();
                auto fmeta = this->functionMetadataManager->getFunctionMetadata(select);

                string arguments;

                for(auto argument : fmeta->getArgumentTypes())
                {
                    arguments+=(argument->getBaseType()+",");
                }
                arguments.pop_back();
                this->exceptionCollector->recordWarn("Select the function " + funcName + "(" + arguments + ") with coercion.");
                return coerce.front();
            }
            else
            {
                auto handle = resolveCachedFunction(candidates,expressionArgumentsTypes);
                if(handle != NULL)
                    return handle;
                else
                {
                    this->exceptionCollector->recordError("Cannot find the function " + funcName + "(" + argumentsString + ")");
                }
            }

        }
        return matchedFunctionHandle;
    }

    shared_ptr<FunctionHandle> resolveCachedFunction(list<shared_ptr<FunctionHandle>> candidates,vector<shared_ptr<TypeSignature>> expressionArgumentsType )
    {

        list<shared_ptr<TypeSignature>> newArgumentTypes;
        for(auto handle =  candidates.begin() ;handle != candidates.end(); handle++)
        {
            shared_ptr<FunctionMetadata> functionMetadata;
            functionMetadata = this->functionMetadataManager->getFunctionMetadata(*handle);

            auto functionArgumentTypes = functionMetadata->getArgumentTypes();

            if(expressionArgumentsType.size() != functionArgumentTypes.size())
                continue;

            bool matched = true;
            for(int i = 0 ;i < expressionArgumentsType.size() ; i++)
            {
                auto functionType = functionArgumentTypes[i]->getBaseType();
                auto eType = expressionArgumentsType[i]->getBaseType();

                if(functionType == "T")
                {
                    newArgumentTypes.push_back(expressionArgumentsType[i]);
                }
                else if(functionType != eType){
                    matched = false;
                    break;
                }
                else
                    newArgumentTypes.push_back(expressionArgumentsType[i]);
            }

            if(matched == true){
                auto cachedHandle = dynamic_pointer_cast<AggregationFunctionHandle>(*handle)->getNewHandleWithNewArguments(newArgumentTypes);
                this->cachedInternalFunctions[cachedHandle] = make_shared<FunctionMetadata>(cachedHandle->getSignature()->getName(),FunctionKind(cachedHandle->getKind()),"",
                                                                                            cachedHandle->getSignature()->getArgumentTypes(),cachedHandle->getSignature()->getArgumentNames(),
                                                                                            cachedHandle->getSignature()->getReturnType());

                return cachedHandle;
            }

        }
        return NULL;
    }



    shared_ptr<FunctionHandle> resolveOperator(string operatorType, vector<Type *> Types)
    {
        list<shared_ptr<FunctionHandle>> candidates = this->functionMetadataManager->resolveFunction(operatorType);

        shared_ptr<FunctionHandle> matchedFunctionHandle = NULL;

        vector<shared_ptr<TypeSignature>> expressionArgumentsTypes;
        for(int i = 0 ;i < Types.size() ; i++)
        {
            if(Types[i] != NULL)
                expressionArgumentsTypes.push_back(TypeSignature::parseTypeSignature(Types[i]->getType()));
            else
                return NULL;
        }

        string argumentsString;
        for(int i = 0 ;i < expressionArgumentsTypes.size() ; i++)
            argumentsString+=(expressionArgumentsTypes[i]->getBaseType()+",");
        if(!argumentsString.empty())
            argumentsString.pop_back();

        for(auto handle =  candidates.begin() ;handle != candidates.end(); handle++)
        {
            shared_ptr<FunctionMetadata> functionMetadata;
            functionMetadata = this->functionMetadataManager->getFunctionMetadata(*handle);

            auto functionArgumentTypes = functionMetadata->getArgumentTypes();

            if(functionArgumentTypes.size() != Types.size()) {
                this->exceptionCollector->recordError("Operator "+operatorType+" arugments number error!");
                return NULL;
            }

            bool matched = true;
            for(int i = 0 ;i < expressionArgumentsTypes.size() ; i++)
            {
                auto functionType = functionArgumentTypes[i]->getBaseType();
                auto eType = expressionArgumentsTypes[i]->getBaseType();

                if(functionType != eType){
                    matched = false;

                    break;
                }
            }

            if(matched == true){
                matchedFunctionHandle = *handle;
                break;
            }

        }

        if(matchedFunctionHandle == NULL) {


            auto coerce = this->functionMetadataManager->resolveOperatorPermitCoercion(candidates,expressionArgumentsTypes);
            if(!coerce.empty())
            {

                auto select = coerce.front();
                auto fmeta = this->functionMetadataManager->getFunctionMetadata(select);

                string arguments;

                for(auto argument : fmeta->getArgumentTypes())
                {
                    arguments+=(argument->getBaseType()+",");
                }
                arguments.pop_back();
                this->exceptionCollector->recordWarn("Select the operator " + operatorType + "(" + arguments + ") with coercion.");

                return coerce.front();
            }
            else
            {
                this->exceptionCollector->recordError("Cannot find the operator " + operatorType + "(" + argumentsString + ")");
            }

        }
        return matchedFunctionHandle;

    }



};



#endif //FRONTEND_FUNCTIONANDTYPERESOLVER_HPP
