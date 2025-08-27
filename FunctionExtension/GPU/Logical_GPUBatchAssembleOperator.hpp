//
// Created by zxk on 8/21/25.
//

#ifndef OLVP_LOGICAL_GPUBATCHASSEMBLEOPERATOR_HPP
#define OLVP_LOGICAL_GPUBATCHASSEMBLEOPERATOR_HPP



#include "../../Operators/LogicalOperators/LogicalOperator.hpp"


#include "GPUBatchAssembleOperator.hpp"

class Logical_GPUBatchAssembleOperator:public LogicalOperator
{

    string name = "Logical_GPUBatchAssembleOperator";


    string operatorId;
public:

    Logical_GPUBatchAssembleOperator(string operatorId) :LogicalOperator("Logical_GPUBatchAssembleOperator"){
        this->operatorId = operatorId;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUBatchAssembleOperator>(this->operatorId,driverContext);
    }

    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<GPUBatchAssembleOperator>(this->operatorId,driverContext);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }


};






#endif //OLVP_LOGICAL_GPUBATCHASSEMBLEOPERATOR_HPP
