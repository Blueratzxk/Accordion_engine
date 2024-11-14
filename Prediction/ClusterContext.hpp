//
// Created by zxk on 7/12/24.
//

#ifndef OLVP_CLUSTERCONTEXT_HPP
#define OLVP_CLUSTERCONTEXT_HPP

#include "../NodeCluster/NodesManager.h"
#include "../Execution/Scheduler/SqlQueryExecution.hpp"


class QueryTasksMap{

    map<string,set<shared_ptr<HttpRemoteTask>>> queryId_ToTasks;
public:
    QueryTasksMap(){

    }

    void addQueryTask(string queryId,set<shared_ptr<HttpRemoteTask>> tasks)
    {
        if(this->queryId_ToTasks.count(queryId) == 0)
        {
            this->queryId_ToTasks[queryId] = tasks;
        }
        else
        {
            for(auto task: tasks)
                this->queryId_ToTasks[queryId].insert(task);
        }
    }
    double getTotalOutputThroughput()
    {
        double total = 0;
        for(auto query: this->queryId_ToTasks) {
            for (auto task: query.second)
                total += task->getTaskInfoFetcher()->getAvgThroughputBytes();
        }
        return total;
    }
    double getLongTotalOutputThroughput()
    {
        double total = 0;
        for(auto query: this->queryId_ToTasks) {
            for (auto task: query.second)
                total += task->getTaskInfoFetcher()->getLongAvgThroughputBytes();
        }
        return total;
    }

    void viewQueryTaskMap()
    {

        for(auto query : this->queryId_ToTasks)
        {
            spdlog::info(TextColor::LIGHT_GREEN("  QueryId:"+query.first));

            map<int,set<shared_ptr<HttpRemoteTask>>> groupByStage;

            for(auto task : query.second)
            {
                int stageId = task->getTaskId()->getStageId().getId();
                if(groupByStage.count(stageId) == 0)
                    groupByStage[stageId] = {task};
                else
                    groupByStage[stageId].insert(task);
            }

            for(auto stage : groupByStage)
            {
                spdlog::info(TextColor::LIGHT_CYAN("    StageId:"+to_string(stage.first)));

                for(auto task : stage.second) {
                    string taskInfo;
                    taskInfo.append(TextColor::LIGHT_BLUE("      TaskId:"+to_string(task->getTaskId()->getId())));
                    taskInfo.append(" TaskCpuUsage:" + to_string(task->getTaskInfoFetcher()->getAvgCpuUsage()));
                    taskInfo.append(" TaskOutputRate:" + to_string(task->getTaskInfoFetcher()->getAvgThroughputBytes()));

                    spdlog::info(taskInfo);

                }

            }


        }
    }

};

class ClusterContext
{

    map<shared_ptr<ClusterNode>,shared_ptr<QueryTasksMap>> nodeTasksMapSnap;
    shared_ptr<map<string,shared_ptr<SqlQueryExecution>>> querys;


public:
    ClusterContext(shared_ptr<map<string,shared_ptr<SqlQueryExecution>>> querys)
    {
        this->querys = querys;
    }


    void resolveStageInfos(string queryId,shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>> stageInfos)
    {

        for(auto cluster : *stageInfos)
        {
            if(nodeTasksMapSnap.count(cluster.first) == 0)
            {
                shared_ptr<QueryTasksMap> queryTasksMap = make_shared<QueryTasksMap>();
                queryTasksMap->addQueryTask(queryId,cluster.second);
                nodeTasksMapSnap[cluster.first] = queryTasksMap;
            }
            else
            {
                nodeTasksMapSnap[cluster.first]->addQueryTask(queryId,cluster.second);
            }
        }

    }

    void gatherInfos()
    {
        for(auto query : *this->querys)
        {
            if(!query.second->isQueryFinished() && query.second->isQueryStart())
            {
                shared_ptr<map<int,shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>>>> stageTasks =
                        query.second->getStagesNodeTaskMap();

                for(auto stage : *stageTasks)
                    this->resolveStageInfos(query.first,stage.second);
            }
        }
    }

    map<shared_ptr<ClusterNode>,shared_ptr<QueryTasksMap>> getTaskInfosByNodes(set<shared_ptr<ClusterNode>> nodes)
    {
        map<shared_ptr<ClusterNode>,shared_ptr<QueryTasksMap>> result;

        for(auto node : nodes)
        {
            result[node] = this->nodeTasksMapSnap[node];
        }
        return result;

    }


    void viewTasksGroupByNode()
    {
        if(this->nodeTasksMapSnap.empty())
        {
            spdlog::info("Node-TaskMap: No query is running now.");
        }
        for(auto node : this->nodeTasksMapSnap)
        {

            spdlog::info(TextColor::LIGHT_RED("Node:"+node.first->getNodeLocation()));
            node.second->viewQueryTaskMap();
        }



    }

};



#endif //OLVP_CLUSTERCONTEXT_HPP
