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
    std::shared_ptr <arrow::Schema> buildInputSchema;
    shared_ptr<LogicalOperator> hashBuilderLogicalOperator = NULL;


public:



    Logical_LookupJoinOperator(std::shared_ptr <arrow::Schema> probeSchema,
                               std::shared_ptr <arrow::Schema> buildInputSchema,
                               std::shared_ptr <arrow::Schema> buildOutputSchema,
                               std::shared_ptr<JoinProbeFactory> joinProbeFactory,
                               std::shared_ptr<LookupSourceFactory> lookupSourceFactory,
                               shared_ptr<LogicalOperator> hashBuilderLogicalOperator) {

        this->joinProbeFactory = joinProbeFactory;
        this->lookupSourceFactory = lookupSourceFactory;
        this->probeSchema = probeSchema;
        this->buildOutputSchema = buildOutputSchema;
        this->buildInputSchema = buildInputSchema;
        this->hashBuilderLogicalOperator = hashBuilderLogicalOperator;


    }

    shared_ptr<LogicalOperator> getHashBuilderLogicalOperator()
    {
        return hashBuilderLogicalOperator;
    }
    std::shared_ptr<LookupSourceFactory> getLookupSourceFactory()
    {
        return this->lookupSourceFactory;
    }

    std::shared_ptr<JoinProbeFactory> getJoinProbeFactory(){return this->joinProbeFactory;}

    std::shared_ptr <arrow::Schema> getProbeSchema(){return probeSchema;}
    std::shared_ptr <arrow::Schema> getBuildOutputSchema(){return buildOutputSchema;}

    std::shared_ptr <arrow::Schema> getBuildInputSchema(){return buildInputSchema;}

    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<LookupJoinOperator>(driverContext,this->probeSchema,this->buildOutputSchema,this->joinProbeFactory,this->lookupSourceFactory);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<LookupJoinOperator>(driverContext,this->probeSchema,this->buildOutputSchema,this->joinProbeFactory,this->lookupSourceFactory);
    }
    string getTypeId(){return name;}



};



#endif //OLVP_LOGICAL_LOOKUPJOINOPERATOR_HPP
