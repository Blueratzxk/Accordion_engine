//
// Created by zxk on 10/18/24.
//

#ifndef FRONTEND_BUILDINFUNCTIONMETAS_HPP
#define FRONTEND_BUILDINFUNCTIONMETAS_HPP

#include "FunctionCalls/SqlFunctionHandle.hpp"
#include "FunctionCalls/AggregationFunctionHandle.hpp"
#include "FunctionMetaData.hpp"
#include "FunctionCalls/AggregationFunction.hpp"
#include "FunctionCalls/AggregationCount.hpp"
#include "FunctionCalls/AggregationSum.hpp"

#include <iostream>
#include <map>

#include "FunctionCalls/DoubleOperators.hpp"
#include "FunctionCalls/BigIntOperators.hpp"
#include "FunctionCalls/BooleanOperators.hpp"

class BuildInFunctionMetas
{
    map<shared_ptr<FunctionHandle>,shared_ptr<FunctionMetadata>> sqlFunctions;
    map<shared_ptr<FunctionHandle>,shared_ptr<FunctionMetadata>> internalFunctions;

    list<shared_ptr<SqlOperator>> sqloperators = {
            make_shared<BigInt_Add>(),
            make_shared<BigInt_Subtract>(),
            make_shared<BigInt_Divide>(),
            make_shared<BigInt_Multiply>(),
            make_shared<BigInt_Modules>(),
            make_shared<BigInt_GreaterThan>(),
            make_shared<BigInt_GreaterThanOrEqual>(),
            make_shared<BigInt_LessThan>(),
            make_shared<BigInt_LessThanOrEqual>(),
            make_shared<BigInt_NotEqual>(),
            make_shared<BigInt_Equal>(),
            make_shared<BigInt_And>(),
            make_shared<BigInt_Or>(),
            make_shared<BigIntCastToDouble>(),


            make_shared<Double_Add>(),
            make_shared<Double_Subtract>(),
            make_shared<Double_Divide>(),
            make_shared<Double_Multiply>(),
            make_shared<Double_Modules>(),
            make_shared<Double_GreaterThan>(),
            make_shared<Double_GreaterThanOrEqual>(),
            make_shared<Double_LessThan>(),
            make_shared<Double_LessThanOrEqual>(),
            make_shared<Double_NotEqual>(),
            make_shared<Double_Equal>(),
            make_shared<Double_And>(),
            make_shared<Double_Or>(),
            make_shared<DoubleCastToBigInt>(),


            make_shared<Boolean_Add>(),
            make_shared<Boolean_Subtract>(),
            make_shared<Boolean_Divide>(),
            make_shared<Boolean_Multiply>(),
            make_shared<Boolean_Modules>(),
            make_shared<Boolean_GreaterThan>(),
            make_shared<Boolean_GreaterThanOrEqual>(),
            make_shared<Boolean_LessThan>(),
            make_shared<Boolean_LessThanOrEqual>(),
            make_shared<Boolean_NotEqual>(),
            make_shared<Boolean_Equal>(),
            make_shared<Boolean_And>(),
            make_shared<Boolean_Or>(),
            make_shared<BooleanCastToBigInt>(),
            make_shared<BooleanCastToDouble>()

    };

    list<shared_ptr<AggregationFunction>> aggregationFunctions = {
            make_shared<AggregationCount>(),
            make_shared<AggregationCountStar>(),
            make_shared<AggregationSumBigInt>(),
            make_shared<AggregationSumDouble>()
    };


public:
    BuildInFunctionMetas()
    {
        init();
    }

    void init()
    {

        for(auto so : this->sqloperators) {
            shared_ptr<SqlOperator> op;
            shared_ptr<FunctionHandle> handle;
            shared_ptr<FunctionMetadata> functionMetadata;

            op = so;
            handle = make_shared<SqlFunctionHandle>(op->getName());
            functionMetadata = make_shared<FunctionMetadata>(op->getName(),FunctionKind(handle->getKind()), op->getOperatorType(),
                                                             op->argumentsToTypeSignatures(), op->getArgumentNames(),
                                                             op->getReturnTypeSignature());
            this->sqlFunctions[handle] = functionMetadata;
        }


        for(auto aggf : this->aggregationFunctions)
        {

            shared_ptr<FunctionHandle> handle;
            shared_ptr<FunctionMetadata> functionMetadata;
            handle = make_shared<AggregationFunctionHandle>(aggf->getFunctionName(),aggf->getSignature());
            functionMetadata = make_shared<FunctionMetadata>(aggf->getSignature()->getName(),FunctionKind(handle->getKind()),"",
                                                             aggf->getSignature()->getArgumentTypes(),aggf->getSignature()->getArgumentNames(),
                                                             aggf->getSignature()->getReturnType());
            this->internalFunctions[handle] = functionMetadata;
        }
    }

    list<shared_ptr<FunctionHandle>> getFunctionsByOperatorType(string operatorType)
    {
        list<shared_ptr<FunctionHandle>> results;
        for(auto op : this->sqlFunctions)
        {
            auto probeType = op.second->getOperatorType();
            if(probeType == operatorType)
            {
                results.push_back(op.first);
            }
        }
        return results;
    }

    list<shared_ptr<FunctionHandle>> getInternalFunctionsByName(string name)
    {
        list<shared_ptr<FunctionHandle>> results;
        for(auto op : this->internalFunctions)
        {
            if(op.second->getName() == name)
            {
                results.push_back(op.first);
            }
        }
        return results;
    }

    shared_ptr<FunctionMetadata> getFunctionMetadata(shared_ptr<FunctionHandle> functionHandle)
    {
        if(functionHandle == NULL)
            return NULL;

        for(auto handle : this->sqlFunctions)
        {
            if(handle.first->getName() == functionHandle->getName())
            {
                if(handle.first->getName() == "SqlFunctionHandle")
                {
                    auto functionIdLeft = dynamic_pointer_cast<SqlFunctionHandle>(handle.first)->getFunctionId();
                    auto functionIdRight = dynamic_pointer_cast<SqlFunctionHandle>(functionHandle)->getFunctionId();
                    if(functionIdLeft == functionIdRight)
                        return handle.second;
                }

            }
        }

        for(auto handle : this->internalFunctions)
        {
            if(handle.first->getName() == functionHandle->getName())
            {
                if(handle.first->getName() == "AggregationFunctionHandle")
                {
                    auto functionIdLeft = dynamic_pointer_cast<AggregationFunctionHandle>(handle.first)->getFunctionId();
                    auto functionIdRight = dynamic_pointer_cast<AggregationFunctionHandle>(functionHandle)->getFunctionId();
                    if(functionIdLeft == functionIdRight)
                        return handle.second;
                }

            }
        }


        return NULL;
    }


};



#endif //FRONTEND_BUILDINFUNCTIONMETAS_HPP
