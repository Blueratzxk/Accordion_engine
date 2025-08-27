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

    string buildSideRemoteSourceOperatorId;

    string operatorId;
public:



    Logical_LookupJoinOperator(string operatorId,
                               std::shared_ptr <arrow::Schema> probeSchema,
                               std::shared_ptr <arrow::Schema> buildInputSchema,
                               std::shared_ptr <arrow::Schema> buildOutputSchema,
                               std::shared_ptr<JoinProbeFactory> joinProbeFactory,
                               std::shared_ptr<LookupSourceFactory> lookupSourceFactory,
                               shared_ptr<LogicalOperator> hashBuilderLogicalOperator,
                               string buildSideRemoteSourceOperatorId) :LogicalOperator("Logical_LookupJoinOperator")  {

        this->joinProbeFactory = joinProbeFactory;
        this->lookupSourceFactory = lookupSourceFactory;
        this->probeSchema = probeSchema;
        this->buildOutputSchema = buildOutputSchema;
        this->buildInputSchema = buildInputSchema;
        this->hashBuilderLogicalOperator = hashBuilderLogicalOperator;
        this->buildSideRemoteSourceOperatorId = buildSideRemoteSourceOperatorId;

        this->operatorId = operatorId;

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

        return std::make_shared<LookupJoinOperator>(this->operatorId,driverContext,this->probeSchema,this->buildOutputSchema,this->joinProbeFactory,
                                                    this->lookupSourceFactory,this->buildSideRemoteSourceOperatorId);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<LookupJoinOperator>(this->operatorId,driverContext,this->probeSchema,this->buildOutputSchema,this->joinProbeFactory,
                                                    this->lookupSourceFactory,this->buildSideRemoteSourceOperatorId);
    }

    string getLogicalOperatorId() override
    {
        return this->operatorId;
    }

};



#endif //OLVP_LOGICAL_LOOKUPJOINOPERATOR_HPP
