//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SOURCEPARTITIONEDSCHEDULER_HPP
#define OLVP_SOURCEPARTITIONEDSCHEDULER_HPP



#include "StageScheduler.hpp"
//#include "SqlStageExecution.hpp"
#include "../../NodeCluster/Node.hpp"
#include "../Task/RemoteTask.hpp"
#include "../../NodeCluster/NodeSelector.hpp"

class SqlStageExecution;
class SourcePartitionedScheduler:public StageScheduler
{

    shared_ptr<SqlStageExecution> stageExecutor;

    map<PlanNodeId,vector<shared_ptr<Split>>> dataSplitSources;
public:
    SourcePartitionedScheduler(shared_ptr<SqlStageExecution> stageExecutor,map<string,vector<shared_ptr<Split>>> dataSplitSources): StageScheduler("SourcePartitionedScheduler")
    {
        this->stageExecutor = stageExecutor;

        for(auto dataSplit: dataSplitSources)
        {
            this->dataSplitSources[PlanNodeId(dataSplit.first)] = dataSplit.second;
        }
    }
    ScheduleResult schedule2()
    {
        vector<shared_ptr<HttpRemoteTask>> newTasks;
        NodeSelector selector;


        for(auto dataSplitSource : this->dataSplitSources) {

            shared_ptr<TaskSource> taskSource;

            set<shared_ptr<ScheduledSplit>> scheduledSplits;

            scheduledSplits.insert(make_shared<ScheduledSplit>(PlanNodeId(dataSplitSource.first),dataSplitSource.second[0]));

            taskSource = make_shared<TaskSource>(PlanNodeId(dataSplitSource.first),scheduledSplits);

            vector<shared_ptr<ClusterNode>> node = selector.getNodesByMinThreadNums(1);
            for(int i = 0 ; i < node.size() ; i++) {
                shared_ptr<HttpRemoteTask> task = stageExecutor->scheduleSplits(node[i],taskSource);
                if(task != NULL)
                    newTasks.push_back(task);
            }

        }


        ScheduleResult sR(newTasks);
        return sR;
    }

    vector<shared_ptr<HttpRemoteTask>> partitionedSchedule(PlanNodeId planNodeId,vector<pair<string,shared_ptr<Split>>> splits)
    {
        vector<shared_ptr<HttpRemoteTask>> newTasks;
        NodeSelector selector;

        vector<shared_ptr<ClusterNode>> nodes;
        vector<shared_ptr<TaskSource>> taskSources;
        for(auto split : splits) {

            shared_ptr<TaskSource> taskSource;

            set<shared_ptr<ScheduledSplit>> scheduledSplits;

            scheduledSplits.insert(make_shared<ScheduledSplit>(planNodeId, split.second));

            taskSource = make_shared<TaskSource>(planNodeId, scheduledSplits);

            shared_ptr<ClusterNode> node = selector.getNodeByAddr(split.first);


            nodes.push_back(node);
            taskSources.push_back(taskSource);


        }

        //let us make mixed stage just one task, and all tablescan pipeline and remotesource pipeline are executed in this task
        //we should gather all tasksources into one tasksource, and schedule one task to executor these tasksources
        if(this->stageExecutor->isRemoteSourceAndTableScanMixedOfStage())
        {
            std::set<std::shared_ptr<ScheduledSplit>> splits;

            for(auto ts : taskSources)
            {
                for(auto s : ts->getSplits())
                {
                    shared_ptr<ScheduledSplit> ssp = make_shared<ScheduledSplit>(s->getPlanNodeId(),s->getSplit());
                    splits.insert(ssp);
                }
            }
            shared_ptr<TaskSource> taskSource = make_shared<TaskSource>(taskSources[0]->getPlanNodeId(),splits);
            vector<shared_ptr<HttpRemoteTask>> task = stageExecutor->scheduleMulSourceSplits({selector.getCoordinator()},{taskSource});
            if (!task.empty()) {
                for (int i = 0; i < task.size(); i++)
                    newTasks.push_back(task[i]);
            }
        }
        else {
            vector<shared_ptr<HttpRemoteTask>> task = stageExecutor->scheduleMulSourceSplits(nodes, taskSources);
            if (!task.empty()) {
                for (int i = 0; i < task.size(); i++)
                    newTasks.push_back(task[i]);
            }
        }

        return newTasks;
    }


    vector<pair<string,shared_ptr<Split>>> analyzeSplitSource(vector<shared_ptr<Split>> splits)
    {
        list<string> hosts;
        vector<pair<string,shared_ptr<Split>>> partitionInfos;
        if(splits.size() > 0) {

            for (auto split: splits) {
                if (split->getConnectorSplit()->getId() == "TpchSplit") {
                    shared_ptr<ConnectorSplit> cs = split->getConnectorSplit();
                    shared_ptr<TpchSplit> tpchSplit = static_pointer_cast<TpchSplit>(cs);
                    hosts = tpchSplit->getPreferredNodes();
                    partitionInfos.push_back({hosts.front(),split});

                }
                else if (split->getConnectorSplit()->getId() == "TpchAutoGenSplit") {
                    shared_ptr<ConnectorSplit> cs = split->getConnectorSplit();
                    shared_ptr<TpchAutoGenSplit> tpchAutoGenSplit = static_pointer_cast<TpchAutoGenSplit>(cs);
                    hosts = tpchAutoGenSplit->getPreferredNodes();
                    partitionInfos.push_back({hosts.front(),split});
                }
            }
        }

        return partitionInfos;

    }


    ScheduleResult schedule()
    {
        vector<shared_ptr<HttpRemoteTask>> newTasks;
        NodeSelector selector;



        for(auto dataSplitSource : this->dataSplitSources) {

            if(!analyzeSplitSource(dataSplitSource.second).empty())
            {
                vector<shared_ptr<HttpRemoteTask>> nTasks = partitionedSchedule(dataSplitSource.first,analyzeSplitSource(dataSplitSource.second));

                for(auto nt : nTasks)
                {
                    newTasks.push_back(nt);
                }
                continue;
            }
            //if table is partitioned, then do partitioned tablescan scheduling (multiple tasks), else just schedule one task to do table scan for a table

            vector<shared_ptr<ClusterNode>> node = selector.getNodesByMinThreadNums(1);
            shared_ptr<TaskSource> taskSource;
            set<shared_ptr<ScheduledSplit>> scheduledSplits;


            scheduledSplits.insert(make_shared<ScheduledSplit>(PlanNodeId(dataSplitSource.first),dataSplitSource.second[0]));
            taskSource = make_shared<TaskSource>(PlanNodeId(dataSplitSource.first),scheduledSplits);
            vector<shared_ptr<HttpRemoteTask>> newTasksTemp = stageExecutor->scheduleMulSplits(node, taskSource);

            for(int i = 0 ; i < newTasksTemp.size() ; i++)
                newTasks.push_back(newTasksTemp[i]);

        }





        stageExecutor->addStageExecutionFinishStateListener();
        ScheduleResult sR(newTasks);
        return sR;
    }


    void close()
    {

    }
};



#endif //OLVP_SOURCEPARTITIONEDSCHEDULER_HPP
