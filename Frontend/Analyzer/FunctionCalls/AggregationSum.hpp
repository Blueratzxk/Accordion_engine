//
// Created by zxk on 11/8/24.
//

#ifndef FRONTEND_AGGREGATIONSUM_HPP
#define FRONTEND_AGGREGATIONSUM_HPP



#include "AggregationFunction.hpp"
#include "../Signature.hpp"

class AggregationSumBigInt : public AggregationFunction
{
    string name;
    FunctionHandle::type FunctionKind;
    shared_ptr<TypeSignature> returnType;
    list<shared_ptr<TypeSignature> > argumentTypes;
public:
    AggregationSumBigInt() : AggregationFunction("sum_BigInt"){
        this->name = "sum";
        this->FunctionKind = FunctionHandle::AGGREGATE;
        this->returnType = make_shared<TypeSignature>(StandardTypes::BIGINT,"");
        this->argumentTypes = {TypeSignature::parseTypeSignature("int64")};

    }


    shared_ptr<Signature> getSignature() override
    {
        return make_shared<Signature>(this->name,this->FunctionKind,this->returnType,this->argumentTypes);
    }


};


class AggregationSumDouble : public AggregationFunction
{
    string name;
    FunctionHandle::type FunctionKind;
    shared_ptr<TypeSignature> returnType;
    list<shared_ptr<TypeSignature> > argumentTypes;
public:
    AggregationSumDouble() : AggregationFunction("sum_Double"){
        this->name = "sum";
        this->FunctionKind = FunctionHandle::AGGREGATE;
        this->returnType = make_shared<TypeSignature>(StandardTypes::DOUBLE,"");
        this->argumentTypes = {TypeSignature::parseTypeSignature("double")};

    }


    shared_ptr<Signature> getSignature() override
    {
        return make_shared<Signature>(this->name,this->FunctionKind,this->returnType,this->argumentTypes);
    }


};
#endif //FRONTEND_AGGREGATIONSUM_HPP
