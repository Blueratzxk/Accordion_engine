//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PARTITIONINGSCHEME_HPP
#define OLVP_PARTITIONINGSCHEME_HPP


#include "Partitioning.hpp"

class PartitioningScheme
{
    shared_ptr<Partitioning> dataPartitioning;
    vector<string> outputLayout;
    vector<string> hashColumn;
    vector<int> bucketToPartition={0};
public:
    PartitioningScheme(){}
    PartitioningScheme(shared_ptr<Partitioning> par){
        this->dataPartitioning = par;
    }

    PartitioningScheme(shared_ptr<Partitioning> par,vector<int> bucketToPartition){
        this->dataPartitioning = par;
    }
    PartitioningScheme(shared_ptr<Partitioning> par,vector<string> outputLayout,vector<string> hashColumn,vector<int> bucketToPartition){
        this->dataPartitioning = par;
        this->outputLayout = outputLayout;
        this->hashColumn = hashColumn;
        this->bucketToPartition = bucketToPartition;
    }

    vector<string> getHashColumns()
    {
        return this->hashColumn;
    }
    shared_ptr<Partitioning> getPartitioning()
    {
        return this->dataPartitioning;
    }

    void withBucketToPartition(vector<int> bucket2Partition)
    {
        this->bucketToPartition = bucket2Partition;
    }
    vector<int>  getBucketToPartition()
    {
        return this->bucketToPartition;
    }

    string Serialize()
    {
        nlohmann::json partitioningSchemeJson;
        partitioningSchemeJson["dataPartitioning"] = this->dataPartitioning->Serialize();
        partitioningSchemeJson["outputLayout"] = this->outputLayout;
        partitioningSchemeJson["hashColumn"] = this->hashColumn;
        partitioningSchemeJson["bucketToPartition"] = this->bucketToPartition;
        string result = partitioningSchemeJson.dump();
        return result;
    }
    shared_ptr<PartitioningScheme> Deserialize(string partitioningStr) {

        if(partitioningStr == "NULL")
            return NULL;

        nlohmann::json partitioningSchemeJson = nlohmann::json::parse(partitioningStr);
        Partitioning p;
        string parString = partitioningSchemeJson["dataPartitioning"];
        return make_shared<PartitioningScheme>(p.Deserialize(parString),partitioningSchemeJson["outputLayout"],partitioningSchemeJson["hashColumn"],partitioningSchemeJson["bucketToPartition"]);

    }


};


#endif //OLVP_PARTITIONINGSCHEME_HPP
