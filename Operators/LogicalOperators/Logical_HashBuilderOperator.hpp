//
// Created by zxk on 5/25/23.
//

#ifndef OLVP_LOGICAL_HASHBUILDEROPERATOR_HPP
#define OLVP_LOGICAL_HASHBUILDEROPERATOR_HPP


#include "LogicalOperator.hpp"

#include "../HashBuilderOperator.hpp"
class Logical_HashBuilderOperator:public LogicalOperator
{



private:


    string name = "Logical_HashBuilderOperator";

    bool sendEndPage = false;


    string joinId;

    vector<int> outputChannels;
    vector<int> hashChannels;




    std::shared_ptr<PartitionedLookupSourceFactory> lookupSourceFactory;

public:



    string getOperatorId() { return this->name; }

    Logical_HashBuilderOperator(string joinId,std::shared_ptr<PartitionedLookupSourceFactory> lookupSourceFactory,
                        vector<int> outputChannels,vector<int> hashChannels) {



        this->lookupSourceFactory = lookupSourceFactory;
        this->outputChannels = outputChannels;
        this->hashChannels = hashChannels;
        this->joinId = joinId;

    }
    std::shared_ptr<Operator> getOperator(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<HashBuilderOperator>(joinId,driverContext,this->lookupSourceFactory->getPartitionAssign(),this->lookupSourceFactory,this->outputChannels,this->hashChannels);
    }
    std::shared_ptr<void> getOperatorNonType(shared_ptr<DriverContext> driverContext) {

        return std::make_shared<HashBuilderOperator>(joinId,driverContext,this->lookupSourceFactory->getPartitionAssign(),this->lookupSourceFactory,this->outputChannels,this->hashChannels);
    }

    string getTypeId(){return name;}



};




#endif //OLVP_LOGICAL_HASHBUILDEROPERATOR_HPP
