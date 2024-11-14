//
// Created by zxk on 6/28/23.
//

#ifndef OLVP_LOGICAL_NESTEDLOOPJOINOPERATOR_HPP
#define OLVP_LOGICAL_NESTEDLOOPJOINOPERATOR_HPP



#include "LogicalOperator.hpp"

#include "../NestedLoopJoinOperator.hpp"
#include "../../Operators/Join/NestedLoopJoin/NestedLoopJoinPagesSupplier.hpp"

class Logical_NestedLoopJoinOperator :public LogicalOperator{




    string name = "Logical_NestedLoopJoinOperator";

    std::shared_ptr <arrow::Schema> probeSchemaIn;
    std::shared_ptr <arrow::Schema> buildSchemaIn;
    std::shared_ptr <arrow::Schema> probeOutputSchemaIn;
    std::shared_ptr <arrow::Schema> buildOutputSchemaIn;

    shared_ptr<NestedLoopJoinPagesSupplier> nestedLoopJoinPagesSupplier;



public:


    Logical_NestedLoopJoinOperator(std::shared_ptr <arrow::Schema> probeSchema,std::shared_ptr <arrow::Schema> buildSchema,std::shared_ptr <arrow::Schema> probeOutputSchema,std::shared_ptr <arrow::Schema> buildOutputSchema,shared_ptr<NestedLoopJoinPagesSupplier> nestedLoopJoinPagesSupplier) {

       this->probeSchemaIn = probeSchema;
       this->buildSchemaIn = buildSchema;
       this->probeOutputSchemaIn = probeOutputSchema;
       this->buildOutputSchemaIn = buildOutputSchema;
       this->nestedLoopJoinPagesSupplier = nestedLoopJoinPagesSupplier;
    }


    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<NestedLoopJoinOperator>(driverContext,this->probeSchemaIn,this->buildSchemaIn,this->probeOutputSchemaIn,this->buildOutputSchemaIn,this->nestedLoopJoinPagesSupplier);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<NestedLoopJoinOperator>(driverContext,this->probeSchemaIn,this->buildSchemaIn,this->probeOutputSchemaIn,this->buildOutputSchemaIn,this->nestedLoopJoinPagesSupplier);
    }
    string getTypeId(){return name;}



};



#endif //OLVP_LOGICAL_NESTEDLOOPJOINOPERATOR_HPP
