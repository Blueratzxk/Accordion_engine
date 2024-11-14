//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_LOGICAL_TOPKOPERATOR_HPP
#define OLVP_LOGICAL_TOPKOPERATOR_HPP

#include "LogicalOperator.hpp"

#include "../TopKOperator.hpp"

class Logical_TopKOperator:public LogicalOperator
{

private:

    string name = "Logical_TopKOperator";


    vector<string> sortKeys;
    vector<TopKOperator::SortOrder> sortOrders;
    int64_t k;



public:


    Logical_TopKOperator(int64_t k,vector<string> sortKeys,vector<TopKOperator::SortOrder> sortOrders) {

        this->sortKeys = sortKeys;
        this->sortOrders = sortOrders;
        this->k = k;

    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<TopKOperator>(driverContext,k,this->sortKeys,this->sortOrders);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<TopKOperator>(driverContext,k,this->sortKeys,this->sortOrders);
    }
    string getTypeId(){return name;}




};




#endif //OLVP_LOGICAL_TOPKOPERATOR_HPP
