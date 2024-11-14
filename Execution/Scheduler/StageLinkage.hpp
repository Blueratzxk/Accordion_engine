//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_STAGELINKAGE_HPP
#define OLVP_STAGELINKAGE_HPP
#include "SqlStageExecution.hpp"
#include "../Buffer/OutputBufferManager.hpp"
#include "../Buffer/BroadcastOutputBufferManager.hpp"
#include "../Buffer/SimpleOutputBufferManager.hpp"
#include "../Buffer/OutputPartitioningBufferManager.hpp"
#include "../Buffer/ShuffleStageBufferManager.hpp"
#include "../Buffer/SimpleShuffleStageBufferManager.hpp"
class StageLinkage
{
    string currentStageFragmentId;
    shared_ptr<SqlStageExecution> parentStage;
    vector<shared_ptr<SqlStageExecution>> childStages;
    vector<shared_ptr<OutputBufferManager>> childOutputBufferManagers;

    shared_ptr<SqlStageExecution> currentStage;
public:
    StageLinkage(string fragmentId, shared_ptr<SqlStageExecution> currentStage, shared_ptr<SqlStageExecution> parentStage,vector<shared_ptr<SqlStageExecution>> childStages){
        this->currentStageFragmentId = fragmentId;
        this->parentStage = parentStage;
        this->childStages = childStages;
        this->currentStage = currentStage;

        for(int i = 0 ; i < this->childStages.size() ; i++)
        {
            //OutputBufferSchema::BufferType type = childStages[i]->getOutputBuffers()->getBufferType();
            shared_ptr<PartitioningScheme> scheme = this->childStages[i]->getFragment()->getPartitionScheme();



            if(scheme->getPartitioning()->getHandle()->equals(*SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION")))
            {
                childOutputBufferManagers.push_back(make_shared<BroadcastOutputBufferManager>());
            }
            else if(scheme->getPartitioning()->getHandle()->equals(*SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"))) {
                childOutputBufferManagers.push_back(make_shared<SimpleOutputBufferManager>());
            }
            else if(scheme->getPartitioning()->getHandle()->equals(*SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF"))) {
                childOutputBufferManagers.push_back(make_shared<OutputPartitioningBufferManager>(scheme));
            }
            else if(scheme->getPartitioning()->getHandle()->equals(*SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF"))) {
                childOutputBufferManagers.push_back(make_shared<OutputPartitioningBufferManager>(scheme,OutputBufferSchema::PAR_REPEATABLE));
            }
            else if(scheme->getPartitioning()->getHandle()->equals(*SystemPartitioningHandle::get("SCALED_HASH_SHUFFLE_STAGE_BUF"))) {
                childOutputBufferManagers.push_back(make_shared<ShuffleStageBufferManager>(scheme));
            }
            else if(scheme->getPartitioning()->getHandle()->equals(*SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF"))) {
                childOutputBufferManagers.push_back(make_shared<SimpleShuffleStageBufferManager>());
            }

            else
            {
                vector<int> partitions = scheme->getBucketToPartition();
                int partitionCount = *max_element(partitions.begin(),partitions.end())+1;
                childOutputBufferManagers.push_back(make_shared<BroadcastOutputBufferManager>());
            }
        }


    }

    vector<shared_ptr<SqlStageExecution>> getChildStages()
    {
        return this->childStages;
    }

    void processScheduleResults(vector<shared_ptr<HttpRemoteTask>> newTasks)
    {
        if(this->parentStage != NULL) {
            this->parentStage->addExchangeLocations(currentStageFragmentId, newTasks);
        }

        vector<OutputBufferId> newOutputBuffers ;
        for(auto newTask : newTasks)
        {
            string tid = to_string(newTask->getTaskId()->getId());
            OutputBufferId outputBufferId(tid);
            newOutputBuffers.push_back(outputBufferId);
        }

        for(int i = 0 ; i < childOutputBufferManagers.size() ; i++)
        {
            auto child = childOutputBufferManagers[i];
            vector<shared_ptr<OutputBufferSchema>> schema = child->addOutputBuffers(newOutputBuffers);
            if(this->currentStage != NULL)
                child->getSchema()->updateTaskGroupMap(this->currentStage->getTaskGroupMap());
            if(schema.size() > 0)
            {
                this->childStages[i]->setOutputBuffers(schema[0]);
            }
        }


    }


    void processScheduleResultsToAddConcurrent(vector<shared_ptr<HttpRemoteTask>> newTasks)
    {
        if(this->parentStage != NULL) {
            this->parentStage->addExchangeLocations(currentStageFragmentId, newTasks);
        }




        vector<OutputBufferId> newOutputBuffers ;
        for(auto newTask : newTasks)
        {
            string tid = to_string(newTask->getTaskId()->getId());
            OutputBufferId outputBufferId(tid);
            newOutputBuffers.push_back(outputBufferId);
        }

        for(int i = 0 ; i < childOutputBufferManagers.size() ; i++)
        {
            auto child = childOutputBufferManagers[i];

            if(this->currentStage != NULL)
                child->getSchema()->updateTaskGroupMap(this->currentStage->getTaskGroupMap());

            if(child->getOutputBufferManagerId().compare("OutputPartitioningBufferManager") == 0 || child->getOutputBufferManagerId().compare("ShuffleStageBufferManager") == 0) {



                vector<shared_ptr<OutputBufferSchema>> schema = child->addOutputBuffers(newOutputBuffers);
                if (schema.size() > 0) {
                    this->childStages[i]->setOutputBuffers(schema[0]);
                }
            }
        }


    }


    void processScheduleResultsToAddOneLocationDynamically(vector<shared_ptr<HttpRemoteTask>> newTasks)
    {
        if(this->parentStage != NULL) {
            this->parentStage->addExchangeLocations(currentStageFragmentId, newTasks);
        }

    }



};
#endif //OLVP_STAGELINKAGE_HPP
