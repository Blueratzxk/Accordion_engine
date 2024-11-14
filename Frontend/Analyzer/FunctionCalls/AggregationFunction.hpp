//
// Created by zxk on 10/20/24.
//

#ifndef FRONTEND_AGGREGATIONFUNCTION_HPP
#define FRONTEND_AGGREGATIONFUNCTION_HPP

#include <string>
#include <iostream>
#include "../Signature.hpp"
using namespace std;
class AggregationFunction {
    string functionName;
public:
    AggregationFunction(string functionName) {
        this->functionName = functionName;
    }

    string getFunctionName()
    {
        return this->functionName;
    }

    virtual shared_ptr<Signature> getSignature() = 0;


};
#endif //FRONTEND_AGGREGATIONFUNCTION_HPP
