//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_LOGICAL_SORTOPERATOR_HPP
#define OLVP_LOGICAL_SORTOPERATOR_HPP


#include "LogicalOperator.hpp"

#include "../SortOperator.hpp"

class Logical_SortOperator:public LogicalOperator
{

private:

    string name = "Logical_SortOperator";

    vector<string> sortKeys;
    vector<SortOperator::SortOrder> sortOrders;



public:


    Logical_SortOperator(vector<string> sortKeys,vector<SortOperator::SortOrder> sortOrders) {

        this->sortKeys = sortKeys;
        this->sortOrders = sortOrders;


    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {


        return std::make_shared<SortOperator>(driverContext,this->sortKeys,this->sortOrders);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {
        return std::make_shared<SortOperator>(driverContext,this->sortKeys,this->sortOrders);
    }
    string getTypeId(){return name;}


};




#endif //OLVP_LOGICAL_SORTOPERATOR_HPP
