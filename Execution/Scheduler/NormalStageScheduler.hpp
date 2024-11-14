//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_NORMALSTAGESCHEDULER_HPP
#define OLVP_NORMALSTAGESCHEDULER_HPP


#include "StageScheduler.hpp"
//#include "SqlStageExecution.hpp"
#include "../../NodeCluster/Node.hpp"
#include "../Task/RemoteTask.hpp"
#include "../../NodeCluster/NodeSelector.hpp"

class SqlStageExecution;
class NormalStageScheduler:public StageScheduler
{

    shared_ptr<SqlStageExecution> stageExecutor;
    vector<shared_ptr<ClusterNode>> partitionToNode;//concurrent and node definition.nodes maybe repeat...
    vector<vector<shared_ptr<ClusterNode>>> nodeGroups;

    double firstPredictionTime = -1;


public:
    NormalStageScheduler(shared_ptr<SqlStageExecution> stageExecutor,vector<shared_ptr<ClusterNode>> partitionToNode,vector<vector<shared_ptr<ClusterNode>>>): StageScheduler("NormalStageScheduler")
    {
        this->stageExecutor = stageExecutor;
        this->partitionToNode = partitionToNode;
        this->nodeGroups = nodeGroups;
    }


    ScheduleResult schedule()
    {
        vector<shared_ptr<HttpRemoteTask>> newTasks;
        for(int i = 0 ; i < this->partitionToNode.size() ; i++)
        {
            shared_ptr<HttpRemoteTask> task = stageExecutor->scheduleTask(this->partitionToNode[i]);
            newTasks.push_back(task);
        }
        stageExecutor->addStageExecutionFinishStateListener();
        ScheduleResult sR(newTasks);
        stageExecutor->recordTaskGroup(sR.getTaskIds());

        return sR;
    }

    ScheduleResult addOneConcurrent()
    {
        vector<shared_ptr<HttpRemoteTask>> newTasks;

        NodeSelector aSelector;
        vector<shared_ptr<ClusterNode>> aNode = aSelector.getNodesByMinThreadNums(1);
        this->partitionToNode.push_back(aNode[0]);
        shared_ptr<HttpRemoteTask> task = stageExecutor->scheduleTask(aNode[0]);
        newTasks.push_back(task);
        ScheduleResult sR(newTasks);
        stageExecutor->recordTaskGroup(sR.getTaskIds());


        return sR;
    }

    ScheduleResult addOneConcurrent(int mode)
    {
        int INITIAL_PLAN_NODES_MODE = 1;

        vector<shared_ptr<HttpRemoteTask>> newTasks;

        NodeSelector aSelector;
        vector<shared_ptr<ClusterNode>> aNode = aSelector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,1);
        this->partitionToNode.push_back(aNode[0]);
        shared_ptr<HttpRemoteTask> task = stageExecutor->scheduleTask(aNode[0]);
        newTasks.push_back(task);
        ScheduleResult sR(newTasks);
        stageExecutor->recordTaskGroup(sR.getTaskIds());


        return sR;
    }

    ScheduleResult addMulConcurrent(int mode,int taskNums)
    {
        int INITIAL_PLAN_NODES_MODE = 1;
        vector<shared_ptr<HttpRemoteTask>> newTasks;

        NodeSelector aSelector;
        vector<shared_ptr<ClusterNode>> aNode = aSelector.getNodesByMinThreadNums(INITIAL_PLAN_NODES_MODE,taskNums);
        for(int i = 0 ; i < aNode.size() ; i++) {
            this->partitionToNode.push_back(aNode[i]);
        }
        for(int i = 0 ; i < aNode.size() ; i++) {
            shared_ptr<HttpRemoteTask> task = stageExecutor->scheduleTask(aNode[i]);
            newTasks.push_back(task);
        }
        ScheduleResult sR(newTasks);
        stageExecutor->recordTaskGroup(sR.getTaskIds());


        return sR;
    }

    ScheduleResult addMulConcurrent(int taskNums)
    {
        vector<shared_ptr<HttpRemoteTask>> newTasks;

        NodeSelector aSelector;
        vector<shared_ptr<ClusterNode>> aNode = aSelector.getNodesByMinThreadNums(taskNums);
        for(int i = 0 ; i < aNode.size() ; i++) {
            this->partitionToNode.push_back(aNode[i]);
        }
        for(int i = 0 ; i < aNode.size() ; i++) {
            shared_ptr<HttpRemoteTask> task = stageExecutor->scheduleTask(aNode[i]);
            newTasks.push_back(task);
        }
        ScheduleResult sR(newTasks);
        stageExecutor->recordTaskGroup(sR.getTaskIds());


        return sR;
    }
    void decreaseOneConcurrentBySourceStage()
    {
        this->stageExecutor->finishATaskBySourceStageTasks();
    }
    void decreaseTaskGroup()
    {
        this->stageExecutor->closeATaskGroup();
    }

    bool hasMulTaskGroup()
    {
        if(this->nodeGroups.size() > 0)
        {
            return true;
        }
        return false;
    }

    void close()
    {

    }
};


#endif //OLVP_NORMALSTAGESCHEDULER_HPP
