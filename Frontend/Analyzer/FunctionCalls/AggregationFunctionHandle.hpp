//
// Created by zxk on 10/20/24.
//

#ifndef FRONTEND_AGGREGATIONFUNCTIONHANDLE_HPP
#define FRONTEND_AGGREGATIONFUNCTIONHANDLE_HPP


#include "FunctionHandle.hpp"
#include "../Signature.hpp"
class AggregationFunctionHandle : public FunctionHandle
{
    string functionId;
    shared_ptr<Signature> signature;
public:
    AggregationFunctionHandle(string functionId,shared_ptr<Signature> signature) : FunctionHandle("AggregationFunctionHandle") {
        this->signature = signature;
        this->functionId = functionId;
    }

    FunctionHandle::type getKind() override
    {
        return signature->getFunctionKind();
    }

    string getFunctionId()
    {
        return this->functionId;
    }

    shared_ptr<AggregationFunctionHandle> getNewHandleWithNewArguments(list<shared_ptr<TypeSignature> > argumentTypes){
        return make_shared<AggregationFunctionHandle>(functionId,
                make_shared<Signature>(this->signature->getName(),this->signature->getFunctionKind(),this->signature->getReturnType(),argumentTypes));
    }


    shared_ptr<Signature> getSignature()
    {
        return this->signature;
    }


};

#endif //FRONTEND_AGGREGATIONFUNCTIONHANDLE_HPP
