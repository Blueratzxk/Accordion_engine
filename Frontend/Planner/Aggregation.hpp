//
// Created by zxk on 10/31/24.
//

#ifndef FRONTEND_AGGREGATION_HPP
#define FRONTEND_AGGREGATION_HPP

#include "Expression/CallExpression.hpp"
class Aggregation {

    string expressionName;
    shared_ptr<CallExpression> call;
    //Optional<RowExpression> filter;
    //Optional<OrderingScheme> orderingScheme;
    //boolean isDistinct;
    //Optional<VariableReferenceExpression> mask;

public:

    Aggregation(shared_ptr<CallExpression> call)
    {
        this->call = call;
    }

    shared_ptr<CallExpression> getCall()
    {
        return this->call;
    }

};


#endif //FRONTEND_AGGREGATION_HPP
