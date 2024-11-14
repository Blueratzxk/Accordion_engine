//
// Created by zxk on 10/20/24.
//

#ifndef FRONTEND_AGGREGATIONCOUNT_HPP
#define FRONTEND_AGGREGATIONCOUNT_HPP

#include "AggregationFunction.hpp"
#include "../Signature.hpp"

class AggregationCount : public AggregationFunction
{
    string name;
    FunctionHandle::type FunctionKind;
    shared_ptr<TypeSignature> returnType;
    list<shared_ptr<TypeSignature> > argumentTypes;
public:
    AggregationCount() : AggregationFunction("count"){
        this->name = "count";
        this->FunctionKind = FunctionHandle::AGGREGATE;
        this->returnType = make_shared<TypeSignature>(StandardTypes::BIGINT,"");
        this->argumentTypes = {TypeSignature::parseTypeSignature("T")};

    }

    AggregationCount(list<shared_ptr<TypeSignature> > argumentTypes) : AggregationFunction("count"){
        this->name = "count";
        this->FunctionKind = FunctionHandle::AGGREGATE;
        this->returnType = make_shared<TypeSignature>(StandardTypes::BIGINT,"");
        this->argumentTypes = argumentTypes;
    }

    shared_ptr<Signature> getSignature() override
    {
        return make_shared<Signature>(this->name,this->FunctionKind,this->returnType,this->argumentTypes);
    }


};


class AggregationCountStar : public AggregationFunction
{
    string name;
    FunctionHandle::type FunctionKind;
    shared_ptr<TypeSignature> returnType;
    list<shared_ptr<TypeSignature> > argumentTypes;
public:
    AggregationCountStar() : AggregationFunction("countStar"){
        this->name = "count";
        this->FunctionKind = FunctionHandle::AGGREGATE;
        this->returnType = make_shared<TypeSignature>(StandardTypes::BIGINT,"");
        this->argumentTypes = {};

    }

    shared_ptr<Signature> getSignature() override
    {
        return make_shared<Signature>(this->name,this->FunctionKind,this->returnType,this->argumentTypes);
    }


};

#endif //FRONTEND_AGGREGATIONCOUNT_HPP
