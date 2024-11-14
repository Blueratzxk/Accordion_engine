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

public:

    Logical_TableScanOperator(std::shared_ptr<PageSourceManager> pageSourceProvider) {
        this->PSM = pageSourceProvider;
    }

    Logical_TableScanOperator(string id,std::shared_ptr<PageSourceManager> pageSourceProvider) {
        this->PSM = pageSourceProvider;
        this->tableScanId = id;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {
        if(this->tableScanId == "NULL")
            return std::make_shared<TableScanOperator>(driverContext,this->PSM);
        else
            return std::make_shared<TableScanOperator>(this->tableScanId,driverContext,this->PSM);

    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        if(this->tableScanId == "NULL")
            return std::make_shared<TableScanOperator>(driverContext,this->PSM);
        else
            return std::make_shared<TableScanOperator>(this->tableScanId,driverContext,this->PSM);

    }
    string getTypeId(){return name;}


};




#endif //OLVP_LOGICAL_TABLESCANOPERATOR_HPP
