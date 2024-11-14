//
// Created by zxk on 5/22/23.
//

#ifndef OLVP_LOGICAL_LOOKUPJOINOPERATOR_HPP
#define OLVP_LOGICAL_LOOKUPJOINOPERATOR_HPP

#include "LogicalOperator.hpp"

#include "../LookupJoinOperator.hpp"

class Logical_LookupJoinOperator :public LogicalOperator{




    string name = "Logical_LookupJoinOperator";
    std::shared_ptr<LookupSourceFactory> lookupSourceFactory = NULL;
    std::shared_ptr<JoinProbeFactory> joinProbeFactory = NULL;

    std::shared_ptr <arrow::Schema> probeSchema;
    std::shared_ptr <arrow::Schema> buildOutputSchema;




public:



    Logical_LookupJoinOperator(std::shared_ptr <arrow::Schema> probeSchema,std::shared_ptr <arrow::Schema> buildOutputSchema,std::shared_ptr<JoinProbeFactory> joinProbeFactory,std::shared_ptr<LookupSourceFactory> lookupSourceFactory) {

        this->joinProbeFactory = joinProbeFactory;
        this->lookupSourceFactory = lookupSourceFactory;
        this->probeSchema = probeSchema;
        this->buildOutputSchema = buildOutputSchema;


    }
    std::shared_ptr<LookupSourceFactory> getLookupSourceFactory()
    {
        return this->lookupSourceFactory;
    }

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<LookupJoinOperator>(driverContext,this->probeSchema,this->buildOutputSchema,this->joinProbeFactory,this->lookupSourceFactory);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<LookupJoinOperator>(driverContext,this->probeSchema,this->buildOutputSchema,this->joinProbeFactory,this->lookupSourceFactory);
    }
    string getTypeId(){return name;}



};



#endif //OLVP_LOGICAL_LOOKUPJOINOPERATOR_HPP
