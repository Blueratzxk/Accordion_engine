//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_LOGICAL_TABLESCANOPERATOR_HPP
#define OLVP_LOGICAL_TABLESCANOPERATOR_HPP

#include "LogicalOperator.hpp"

#include "../TableScanOperator.hpp"
using namespace std;

class Logical_TableScanOperator:public LogicalOperator {

    string name = "Logical_TableScanOperator";
    std::shared_ptr<PageSourceManager> PSM;

    string tableScanId = "NULL";

    string operatorId;
public:

    Logical_TableScanOperator(string operatorId,std::shared_ptr<PageSourceManager> pageSourceProvider) : LogicalOperator("Logical_TableScanOperator") {
        this->PSM = pageSourceProvider;
        this->operatorId = operatorId;
    }

    Logical_TableScanOperator(string operatorId,string id,std::shared_ptr<PageSourceManager> pageSourceProvider) : LogicalOperator("Logical_TableScanOperator"){
        this->PSM = pageSourceProvider;
        this->tableScanId = id;
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        if(this->tableScanId == "NULL")
            return std::make_shared<TableScanOperator>(this->operatorId,driverContext,this->PSM);
        else
            return std::make_shared<TableScanOperator>(this->operatorId,this->tableScanId,driverContext,this->PSM);

    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        if(this->tableScanId == "NULL")
            return std::make_shared<TableScanOperator>(this->operatorId,driverContext,this->PSM);
        else
            return std::make_shared<TableScanOperator>(this->operatorId,this->tableScanId,driverContext,this->PSM);

    }
    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }


};




#endif //OLVP_LOGICAL_TABLESCANOPERATOR_HPP
