//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_SQLSTAGEEXECUTION_HPP
#define OLVP_SQLSTAGEEXECUTION_HPP


#include "../../Planner/Fragment.hpp"
#include "SqlStageExecutionStateMachine.hpp"
#include "../Task/RemoteTaskFactory.hpp"
#include "../Task/RemoteTask.hpp"
#include "../../NodeCluster/Node.hpp"
#include "../../Frontend/PlanNode/PlanNodeId.hpp"
#include "../../Frontend/PlanNode/RemoteSourceNode.hpp"

#include "../../Descriptor/TaskInterfere/TaskIntraParaUpdateRequest.hpp"
#include "../../Descriptor/TaskInterfere/TaskBufferOperatingRequest.hpp"

#include "../../Split/RemoteSplit.hpp"

#include "QueryInfos/StageInfo.hpp"
#include "../Event/SimpleEvent.hpp"
#include <bitset>
class taskCpuNetUsageInfo
{
public:
    int taskId;
    float totalUsage;
    float shuffleUsage;
    float driverUsage;
    float throughputBytes;
    int tb_tup;
    int tb_tdown;
    int eb_tup;
    int eb_tdown;
};

class SqlStageExecution:public enable_shared_from_this<SqlStageExecution>
{
    string queryId;
    int stageExecutionId;
    int stageId;

    shared_ptr<Session> session;

    int nextTaskGroupId = 0;


    shared_ptr<StageExecutionStateMachine> state;
    shared_ptr<PlanFragment> fragment;
    RemoteTaskFactory taskFactory;
    map<shared_ptr<ClusterNode>,vector<shared_ptr<HttpRemoteTask>>> NodeTaskNap;
    vector<TaskId> allTasks;
    vector<TaskId> finishedTasks;
    atomic<int> nextTaskId = -1;
    multimap<PlanNodeId,shared_ptr<HttpRemoteTask>> sourceTasks;//newest sourceTasks for every own task
    map<shared_ptr<ClusterNode>, vector<shared_ptr<HttpRemoteTask>>> tasks;//one node maybe runs multi task for a stage
    map<string,PlanNode*> exchangeSources;

    map<int,list<TaskId>> taskGroups;

    mutex tasksLock;

    shared_ptr<OutputBufferSchema> outputBufferSchema = NULL;

    atomic<bool> outputBufferSeted = false;

    shared_ptr<Event> simpleEvent;

public:
    SqlStageExecution(){}
    SqlStageExecution(shared_ptr<Session> session,string queryId,int stageExecutionId,int stageId,shared_ptr<PlanFragment> fragment){
        this->stageExecutionId = stageExecutionId;
        this->stageId = stageId;
        this->queryId = queryId;
        this->session = session;
        this->fragment = fragment;
        this->simpleEvent = make_shared<SimpleEvent>();

        this->outputBufferSchema;

        //Get remoteSourceNodes from fragment
        vector<PlanNode*> remoteSources = fragment->getRemoteSourceNodes();

        //We can know how many remoteSources a fragment have,and let this fragment communicate with these remoteSources.

        for(int i = 0 ; i < remoteSources.size() ; i++)
        {
            string fragmentId = ((RemoteSourceNode*)remoteSources[i])->getSourceFragmentId();
            exchangeSources[fragmentId] = remoteSources[i];
        }

    }

    shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>> getActiveTaskNodeMap()
    {
        shared_ptr<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>> result =
                make_shared<map<shared_ptr<ClusterNode>, set<shared_ptr<HttpRemoteTask>>>>();

        tasksLock.lock();


        for(auto node : this->tasks)
        {
           if((*result).count(node.first) == 0) {
               (*result)[node.first] = {};
           }
           for(auto task : node.second)
               if(!task->isDone())
                   (*result)[node.first].insert(task);
        }

        tasksLock.unlock();

        return result;
    }


    bool isBufferNeedPartitioning()
    {
        return this->outputBufferSchema->isPartitioning_Buffer();
    }

    bool isHavingAdaptiveDOP()
    {
        return this->outputBufferSchema->isPartitioning_Buffer();
    }

    shared_ptr<Session> getSession()
    {
        return this->session;
    }
    shared_ptr<Event> getStateChangeListener()
    {
        return this->state->getStateChangeListener();
    }

    double getStageThroughput()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();

        double totalThroughput = 0.0;
        for(auto task : allTasks) {
            if(!task->isDone()) {
                totalThroughput += task->getTaskInfoFetcher()->getThroughput();
            }
        }


        return totalThroughput;
    }

    list<taskCpuNetUsageInfo> getStageCpuUsages()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        list<taskCpuNetUsageInfo> usages;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();



        for(auto task : allTasks) {
            if(!task->isDone()) {
                taskCpuNetUsageInfo tci;
                tci.taskId = task->getTaskId()->getId();
                tci.totalUsage = task->getTaskInfoFetcher()->getAvgCpuUsage();
                tci.shuffleUsage = task->getTaskInfoFetcher()->getAvgShuffleCpuUsage();
                tci.driverUsage = task->getTaskInfoFetcher()->getAvgDriverCpuUsage();
                tci.throughputBytes = task->getTaskInfoFetcher()->getAvgThroughputBytes();

                auto bufferInfo = task->getTaskInfoFetcher()->getBufferInfoDesc();
                tci.tb_tup = bufferInfo.getTb_TUp();
                tci.tb_tdown = bufferInfo.getTb_TDown();
                tci.eb_tup = bufferInfo.getEb_TUp();
                tci.eb_tdown = bufferInfo.getEb_TDown();
                usages.push_back(tci);
            }
        }


        return usages;
    }

    double getSingleTaskOfTheStageCpuUsage()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        list<taskCpuNetUsageInfo> usages;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();



        double all = 0.0;
        int num = 0;
        for(auto task : allTasks) {
            if(!task->isDone()) {
                auto totalUsage = task->getTaskInfoFetcher()->getAvgCpuUsage();
                all += totalUsage;
                num++;
            }
        }
        if(num == 0)
            return 0;

        return all/num;
    }

    double getRemainingCpuUsageRatioOfStageByTaskThreadNums()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        double usage = this->getSingleTaskOfTheStageCpuUsage();

        list<taskCpuNetUsageInfo> usages;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();


        double maxUsageOfTask = 0.0;
        if(!allTasks.empty())
        {
            for(auto task:allTasks)
            {
                if(!task->isDone()) {
                    maxUsageOfTask = task->getTaskInfoFetcher()->getAllThreadsNums()*100.0;
                }
            }
        }
        double ratio;
        if(usage > 0)
            ratio = (maxUsageOfTask-usage)/usage;
        else
            ratio = 0;
        return ratio;
    }

    string getStageThroughputInfo()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();

        double totalThroughput = 0.0;
        double totalRemainingTuples = 0;
        for(auto task : allTasks) {
            if(!task->isDone()) {
                totalThroughput += task->getTaskInfoFetcher()->getThroughput();
                totalRemainingTuples += task->getTaskInfoFetcher()->getRemainingTuples();
            }
        }

        nlohmann::json info;
        info["TPV"] = totalThroughput;
        info["RTV"] = totalRemainingTuples;

        return info.dump();
    }

    string getStageInfo()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();

        for(auto task : allTasks) {
            shared_ptr<TaskInfo> taskInfo = task->getTaskInfo();
            if(taskInfo != NULL)
                taskInfos.push_back(taskInfo);
        }


        shared_ptr<StageInfo> stageInfo = make_shared<StageInfo>(to_string(this->stageId),this->allTasks.size(),
                                                                 this->state->getStateToString(),this->fragment,taskInfos);
        return stageInfo->ToString();
    }

   set<shared_ptr<ClusterNode>> getCurrentStageNodes()
   {
       set<shared_ptr<ClusterNode>> nodes;
       tasksLock.lock();
        for(auto task : this->tasks)
        {
            nodes.insert(task.first);
        }
       tasksLock.unlock();
        return nodes;
   }

    double getAvgStageThroughput() {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();


        int taskCount = 0;
        double totalThroughput = 0.0;
        for (auto task: allTasks) {
                totalThroughput += task->getTaskInfoFetcher()->getAvgThroughput();
                taskCount++;

        }

        return totalThroughput/allTasks.size();
    }



    map<int,string> getBuildRecords()
    {
        map<int,string> buildRecords;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();
        for(auto task : allTasks) {
            string bTime = task->getTaskInfoFetcher()->getBuildRecord();
            if(bTime != "-1")
                buildRecords[task->getTaskId()->getId()] = bTime;
        }
        return buildRecords;
    }

    double getMaxHashTableBuildTimeofTasks()
    {
        double max = -1;
        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();
        for(auto task : allTasks) {
            string bTime = task->getTaskInfoFetcher()->getBuildRecord();
            if(bTime != "-1") {
                double m = task->getTaskInfoFetcher()->getBuildTime();
                if( m > max)
                    max = m;
            }
        }
        return max;
    }

    double getMaxHashTableBuildComputingTimeofTasks()
    {
        double max = -1;
        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();
        for(auto task : allTasks) {
            string bTime = task->getTaskInfoFetcher()->getBuildRecord();
            if(bTime != "-1") {
                double m = task->getTaskInfoFetcher()->getBuildComputingTime();
                if( m > max)
                    max = m;
            }
        }
        return max;
    }

    long getTotalBytesTasksOutput()
    {
        long total = 0;
        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();
        for(auto task : allTasks) {
            long bytes = task->getTaskInfoFetcher()->getTaskInfo()->getTaskInfoDescriptor()->getTaskThroughputInfo().getTotalTuplesBytes();
            total += bytes;
        }
        return total;
    }

    double getRemainingTupleCount()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();

        double totalRemainingTuples = 0;
        for(auto task : allTasks) {
            if(!task->isDone()) {
                totalRemainingTuples += task->getTaskInfoFetcher()->getRemainingTuples();
            }
        }
        return totalRemainingTuples;
    }
    double getLastEnqueuedTupleCounts()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();

        double totalLastEnqueuedTupleCounts = 0;
        for(auto task : allTasks) {
            if(!task->isDone()) {
                totalLastEnqueuedTupleCounts += task->getTaskInfoFetcher()->getLastEnqueuedTuples();
            }
        }
        return totalLastEnqueuedTupleCounts;
    }
    double getRemainingTupleRate()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();

        double totalAllRemainingTuples = 0;
        double totalRemainingTuples = 0;
        double maxRequestCount = 0;
        for(auto task : allTasks) {
            if(!task->isDone()) {
                totalAllRemainingTuples += task->getTaskInfoFetcher()->getMaxRemainingTuple();
                totalRemainingTuples += task->getTaskInfoFetcher()->getRemainingTuples();
                int count = task->getTaskInfoFetcher()->getRequestCounter();
                if(count > maxRequestCount)
                    maxRequestCount = count;

            }
        }

        if(maxRequestCount == 0 || totalRemainingTuples == 0 || totalAllRemainingTuples == 0)
            return 0;

        return (totalAllRemainingTuples-totalRemainingTuples)/ (maxRequestCount * 100);
    }

    double getNonTableScanRemainingTupleRate()
    {
        vector<shared_ptr<TaskInfo>> taskInfos;

        vector<shared_ptr<HttpRemoteTask>> allTasks = this->getAllTasks();

        double totalAllRemainingTuples = 0;
        double totalRemainingTuples = 0;
        double maxRequestCount = 0;
        for(auto task : allTasks) {
            if(!task->isDone()) {
                totalAllRemainingTuples += task->getTaskInfoFetcher()->getLastEnqueuedTuples();
                totalRemainingTuples += task->getTaskInfoFetcher()->getRemainingBufferTuples();
                int count = task->getTaskInfoFetcher()->getBufferRequestCounter();
                if(count > maxRequestCount)
                    maxRequestCount = count;

            }
        }

        if(maxRequestCount == 0 || totalRemainingTuples == 0 || totalAllRemainingTuples == 0)
            return 0;

        return (totalAllRemainingTuples-totalRemainingTuples)/ (maxRequestCount * 100);
    }
    double getNonTableScanRemainingTime()
    {
        double remainingTuple = this->getLastEnqueuedTupleCounts();
        double avgThroughput = this->getNonTableScanRemainingTupleRate();


        double remainingTime;
        if(remainingTuple == 0 || avgThroughput == 0)
            remainingTime = 0;
        else
            remainingTime = remainingTuple/avgThroughput;

        //   if(lastTuples != 0 && avgThroughput != 0)
        //      remainingTime = lastTuples / avgThroughput;

        return remainingTime;
    }
    double getRemainingTime()
    {
        double remainingTuple = this->getRemainingTupleCount();
        double avgThroughput = this->getRemainingTupleRate();
        double lastTuples = this->getLastEnqueuedTupleCounts();


        double remainingTime;
        if(remainingTuple == 0 || avgThroughput == 0)
            remainingTime = 0;
        else
            remainingTime = remainingTuple/avgThroughput;

     //   if(lastTuples != 0 && avgThroughput != 0)
      //      remainingTime = lastTuples / avgThroughput;

        return remainingTime;
    }

    SqlStageExecution(string queryId,int stageExecutionId,int stageId,shared_ptr<PlanFragment> fragment,shared_ptr<OutputBufferSchema> schema){
        this->stageExecutionId = stageExecutionId;
        this->fragment = fragment;

        this->outputBufferSchema = schema;

        vector<PlanNode*> remoteSources = fragment->getRemoteSourceNodes();

        for(int i = 0 ; i < remoteSources.size() ; i++)
        {
            string fragmentId = ((RemoteSourceNode*)remoteSources[i])->getSourceFragmentId();
            exchangeSources[fragmentId] = remoteSources[i];
        }
        this->state = make_shared<StageExecutionStateMachine>();
        this->state->start();
    }

    map<int,int> getTaskGroupMap()
    {
        map<int,int> tgm;

        for(auto t : this->taskGroups)
        {
            tgm[t.first] = t.second.size();
        }
        return tgm;
    }

    shared_ptr<Event> getEventListener()
    {
        return this->simpleEvent;
    }
    bool isStageScalable()
    {
        if(this->fragment->getPartitionHandle() == NULL)
            return false;
        if(this->fragment->getPartitionHandle()->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0)
        {
            auto handle = this->fragment->getPartitionHandle();
            return static_pointer_cast<SystemPartitioningHandle>(handle->getConnectorHandle())->isScalable();
        }
        else
            return false;

    }
    void beginScheduling()
    {
        this->state = make_shared<StageExecutionStateMachine>();
        this->state->start();
       // this->addStageExecutionFinishStateListener();
    }

    void addStageExecutionFinishStateListener(){

        thread process([](shared_ptr<SqlStageExecution> stageExecution) {
            while (true) {

                stageExecution->getEventListener()->listen();
                vector<shared_ptr<HttpRemoteTask>> allTasks = stageExecution->getAllTasks();

                int finishedCount = 0;
                for(auto remoteTask : allTasks)
                {
                    if(remoteTask->isDone())
                    {
                        finishedCount++;
                    }
                }
                if(finishedCount == allTasks.size()) {
                    stageExecution->state->finished();
                    double runningTime = stageExecution->state->getRunningTime();
                    spdlog::info("Stage ---> "+stageExecution->queryId+"$"+to_string(stageExecution->stageId) + " finished! RunningTime:"+to_string(runningTime));


                    break;
                }

            }
        },shared_from_this());
        process.detach();

    }

    bool setedOutputBuffer()
    {
        return this->outputBufferSeted;
    }
    void closeAllTasks()
    {
       vector<shared_ptr<HttpRemoteTask>> allRemoteTasks;
        allRemoteTasks = this->getAllTasks();

       for(auto task: allRemoteTasks)
       {
           task->close();
       }
    }

    vector<shared_ptr<HttpRemoteTask>> getSourceTasks()
    {
        vector<shared_ptr<HttpRemoteTask>> remoteTasks;
        multimap<PlanNodeId,shared_ptr<HttpRemoteTask>>::iterator iter = this->sourceTasks.begin();
        while (iter != this->sourceTasks.end())
        {
            remoteTasks.push_back(iter->second);
            ++iter;
        }
        return remoteTasks;
    }
    TaskId getMinTaskId()
    {
        tasksLock.lock();
        int min = INT32_MAX;
        int minTaskId;
        TaskId minT;
        for(int i = 0 ; i < this->allTasks.size() ; i++)
        {
            TaskId id = allTasks[i];

            int tid = id.getId();
            if(tid < min)
            {
                min = tid;
                minTaskId = id.getId();
                minT = id;
            }
        }
        tasksLock.unlock();
        return minT;

    }
    void finishATaskBySourceStageTasks()
    {
        TaskId minId = this->getMinTaskId();
        vector<shared_ptr<HttpRemoteTask>> sourceStageTasks = this->getSourceTasks();
        vector<string> bufferIds = {to_string(minId.getId())};

        for(int i = 0 ; i < sourceStageTasks.size() ; i++)
        {
            shared_ptr<TaskBufferOperatingRequest> request = make_shared<TaskBufferOperatingRequest>(TaskBufferOperatingRequest::CLOSE_BUFFER,bufferIds);
            sourceStageTasks[i]->operateOutputBuffer(request);
        }

        tasksLock.lock();
        for(int i = 0 ; i < this->allTasks.size() ; i++)
        {
            if(this->allTasks[i].getId() == minId.getId())
            {
                this->allTasks.erase(this->allTasks.begin()+i);
                this->finishedTasks.push_back(minId);
            }
        }
        tasksLock.unlock();


    }



    int getNextTaskId()
    {
        this->nextTaskId++;
        return (this->nextTaskId);
    }

    void addNodeTaskMap(shared_ptr<ClusterNode> node,shared_ptr<HttpRemoteTask> remote)
    {
        if(this->NodeTaskNap.find(node) == this->NodeTaskNap.end())
        {
            vector<shared_ptr<HttpRemoteTask>> tasks;
            this->NodeTaskNap[node] = tasks;
            this->NodeTaskNap[node].push_back(remote);
        }
        else
            this->NodeTaskNap[node].push_back(remote);
    }
    void addTask(shared_ptr<ClusterNode> node,shared_ptr<HttpRemoteTask> remote)
    {
        tasksLock.lock();
        if(this->tasks.find(node) == this->tasks.end())
        {
            vector<shared_ptr<HttpRemoteTask>> tasks;
            this->tasks[node] = tasks;
            this->tasks[node].push_back(remote);
            allTasks.push_back(*remote->getTaskId());
        }
        else {
            this->tasks[node].push_back(remote);
            allTasks.push_back(*remote->getTaskId());
        }
        tasksLock.unlock();
    }

    void updateTasksIntraPara(shared_ptr<TaskIntraParaUpdateRequest> request)
    {
        bool tasksEmpty = false;
        tasksLock.lock();
        tasksEmpty = tasks.empty();
        tasksLock.unlock();
        if(!tasksEmpty) {
            vector<shared_ptr<HttpRemoteTask>> tasks = getAllTasks();
            for(auto task : tasks)
            {
                task->updateTaskIntraParallelism(request);
            }
        }

    }


    bool isRemoteSourceAndTableScanMixedOfStage()
    {
        return this->fragment->getRemoteSourceNodes().size() > 0 && this->fragment->getTableScanNodes().size() > 0;
    }


    shared_ptr<HttpRemoteTask> scheduleTask(shared_ptr<ClusterNode> node)
    {

        spdlog::debug("Stage "+to_string(this->stageId) + " starts scheduling task!");
        TaskId taskId = TaskId(this->queryId,this->stageExecutionId,this->stageId,getNextTaskId());
        shared_ptr<TaskSource> task_Sources = this->sourceTasksTo_taskSources(taskId);

        string location;
        auto newTaskId = make_shared<TaskId>(taskId.getQueryId().getId(),taskId.getStageExecutionId().getId(),taskId.getStageId().getId(),taskId.getId());


        shared_ptr<HttpRemoteTask> remoteTask = taskFactory.createRemoteTask(this->simpleEvent,newTaskId,fragment,node->getNodeLocation(),this->outputBufferSchema,
                                                                             task_Sources==NULL?TaskSource::getEmptyTaskSource():task_Sources,this->session);

        addNodeTaskMap(node,remoteTask);
        addTask(node,remoteTask);
        remoteTask->start();
        spdlog::debug(to_string(this->stageId) + "schedules task OK!");
        return remoteTask;
    }

    shared_ptr<HttpRemoteTask> scheduleSplits(shared_ptr<ClusterNode> node,shared_ptr<TaskSource> tss)
    {
        shared_ptr<HttpRemoteTask> remoteTask = NULL;
        if(tasks.empty()) {


            TaskId taskId = TaskId(this->queryId,this->stageExecutionId,this->stageId,getNextTaskId());
            auto newTaskId = make_shared<TaskId>(taskId.getQueryId().getId(),taskId.getStageExecutionId().getId(),taskId.getStageId().getId(),taskId.getId());



            string location;
            remoteTask = taskFactory.createRemoteTask(this->simpleEvent,newTaskId, fragment, node->getNodeLocation(),this->outputBufferSchema,TaskSource::getEmptyTaskSource(),this->session);
            addNodeTaskMap(node, remoteTask);
            addTask(node, remoteTask);
            remoteTask->start();
        }
        else
        {

            vector<shared_ptr<HttpRemoteTask>> tasks = getAllTasks();




            for(auto task : tasks) {

                /*if table stage also have remotesource,we should schedule remotesource too*/
                shared_ptr<TaskSource> task_Sources = this->sourceTasksTo_taskSources(*task->getTaskId());
                for (auto ts: task_Sources->getSplits())
                {
                    tss->addSplit(ts);
                }
                /*if table stage also have remotesource,we should schedule remotesource too*/



                task->addSplits(tss);
            }
        }

        return remoteTask;
    }

    vector<shared_ptr<HttpRemoteTask>> scheduleMulSplits(vector<shared_ptr<ClusterNode>> node,shared_ptr<TaskSource> tss)
    {
        shared_ptr<HttpRemoteTask> remoteTask = NULL;
        vector<shared_ptr<HttpRemoteTask>> remoteTasks;
        if(tasks.empty()) {
            for(int i = 0 ; i < node.size() ; i++) {

                TaskId taskId = TaskId(this->queryId,this->stageExecutionId,this->stageId,getNextTaskId());
                auto newTaskId = make_shared<TaskId>(taskId.getQueryId().getId(),taskId.getStageExecutionId().getId(),taskId.getStageId().getId(),taskId.getId());



                string location;
                remoteTask = taskFactory.createRemoteTask(this->simpleEvent,newTaskId, fragment, node[i]->getNodeLocation(),this->outputBufferSchema,tss,this->session);
                addNodeTaskMap(node[i], remoteTask);
                addTask(node[i], remoteTask);
                remoteTask->start();
                remoteTasks.push_back(remoteTask);
            }
        }
        else
        {
            vector<shared_ptr<HttpRemoteTask>> tasks = getAllTasks();
            for(auto task : tasks)
            {

                task->addSplits(tss);
            }
        }

        return remoteTasks;
    }

    vector<shared_ptr<HttpRemoteTask>> scheduleMulSourceSplits(vector<shared_ptr<ClusterNode>> node,vector<shared_ptr<TaskSource>> tss)
    {
        shared_ptr<HttpRemoteTask> remoteTask = NULL;
        vector<shared_ptr<HttpRemoteTask>> remoteTasks;
        if(tasks.empty()) {
            for(int i = 0 ; i < node.size() ; i++) {

                TaskId taskId = TaskId(this->queryId,this->stageExecutionId,this->stageId,getNextTaskId());
                auto newTaskId = make_shared<TaskId>(taskId.getQueryId().getId(),taskId.getStageExecutionId().getId(),taskId.getStageId().getId(),taskId.getId());


                /*if table stage also have remotesource,we should schedule remotesource too*/
                shared_ptr<TaskSource> task_Sources = this->sourceTasksTo_taskSources(taskId);
                for (auto ts: task_Sources->getSplits())
                {
                    tss[i]->addSplit(ts);
                }
                /*if table stage also have remotesource,we should schedule remotesource too*/


                string location;
                remoteTask = taskFactory.createRemoteTask(this->simpleEvent,newTaskId, fragment, node[i]->getNodeLocation(),this->outputBufferSchema,tss[i],this->session);
                addNodeTaskMap(node[i], remoteTask);
                addTask(node[i], remoteTask);
                remoteTask->start();
                remoteTasks.push_back(remoteTask);
            }
        }
        else
        {
            vector<shared_ptr<HttpRemoteTask>> tasks = getAllTasks();
            for(auto task : tasks)
            {
                /*if table stage also have remotesource,we should schedule remotesource too*/
                shared_ptr<TaskSource> task_Sources = this->sourceTasksTo_taskSources(*task->getTaskId());
                for (auto ts: task_Sources->getSplits())
                {
                    tss[0]->addSplit(ts);
                }
                /*if table stage also have remotesource,we should schedule remotesource too*/

                task->addSplits(tss[0]);
            }
        }

        return remoteTasks;
    }

    void recordTaskGroup(list<TaskId> taskGroup)
    {
        for(auto taskId : taskGroup){
            this->taskGroups[this->nextTaskGroupId].push_back(taskId);
        }

        this->nextTaskGroupId++;

    }

    void closeATaskGroup()
    {
        vector<shared_ptr<HttpRemoteTask>> sourceStageTasks = this->getSourceTasks();

        int minTg = INT32_MAX;
        if(this->taskGroups.empty()){
            spdlog::info("No task groups!");
            return;
        }
        for(auto tg : this->taskGroups)
        {
            if(tg.first < minTg)
                minTg = tg.first;
        }

        vector<string> tgIds;
        for(auto taskId : this->taskGroups[minTg])
        {
            tgIds.push_back(to_string(taskId.getId()));
        }

        for(int i = 0 ; i < sourceStageTasks.size() ; i++)
        {
            shared_ptr<TaskBufferOperatingRequest> request = make_shared<TaskBufferOperatingRequest>(TaskBufferOperatingRequest::CLOSE_BUFFER_GROUP,tgIds);
            sourceStageTasks[i]->operateOutputBuffer(request);
        }

        tasksLock.lock();
        for(auto taskId : this->taskGroups[minTg]) {
            for (int i = 0; i < this->allTasks.size(); i++) {
                if (this->allTasks[i].getId() == taskId.getId()) {
                    this->allTasks.erase(this->allTasks.begin() + i);
                    this->finishedTasks.push_back(taskId);
                }
            }
        }
        tasksLock.unlock();

        map<int,list<TaskId>>::iterator iter=this->taskGroups.find(minTg);
        this->taskGroups.erase(iter);
    }

    int getStageExecutionId()
    {
        return this->stageExecutionId;
    }
    StageId getStageId()
    {
        return StageId(QueryId(this->queryId),this->stageId);
    }

    shared_ptr<StageExecutionStateMachine> getState()
    {
        return this->state;
    }

    shared_ptr<PlanFragment> getFragment()
    {
        return this->fragment;
    }

    shared_ptr<OutputBufferSchema> getOutputBuffers()
    {
        return this->outputBufferSchema;
    }



    void transitionToFinishedTaskScheduling() {

    }
    void transitionToSchedulingSplits()
    {

    }

    shared_ptr<TaskSource> sourceTasksTo_taskSources(TaskId ownTaskId)
    {
        shared_ptr<TaskSource> task_Sources = NULL;
        map<string,vector<shared_ptr<HttpRemoteTask>>> splits;
        for (auto sourceTask : this->sourceTasks) {
            PlanNodeId id = sourceTask.first;
            splits[id.get()].push_back(sourceTask.second);
        }


        set<shared_ptr<ScheduledSplit>> scheduledSplits;

        string oneOfPlanNodeId;

        for(auto source : splits)
        {

            for(auto split : source.second) {

                auto remoteSplit = make_shared<RemoteSplit>(split->getTaskId(),make_shared<Location>(split->getIP(),"9081",to_string(ownTaskId.getId())));

                scheduledSplits.insert(make_shared<ScheduledSplit>(PlanNodeId(source.first), make_shared<Split>(ConnectorId("remote"),remoteSplit)));
                oneOfPlanNodeId = source.first;
            }
        }

        shared_ptr<TaskSource> taskSource = make_shared<TaskSource>(PlanNodeId(oneOfPlanNodeId),scheduledSplits);

        return taskSource;

    }
    void addExchangeLocations(string planFragmentId,vector<shared_ptr<HttpRemoteTask>> source_Tasks) {


        PlanNode *remoteSource = exchangeSources[planFragmentId];

        for(int i = 0 ; i < source_Tasks.size() ; i++){
            this->sourceTasks.emplace(PlanNodeId(remoteSource->getId()),source_Tasks[i]);
        }

        for (auto ownTask : getAllTasks()) {
            shared_ptr<TaskSource> task_Sources = this->sourceTasksTo_taskSources(*ownTask->getTaskId());
            ownTask->addSplits(task_Sources);
        }


    }

    vector<shared_ptr<HttpRemoteTask>> getAllTasks()
    {
        tasksLock.lock();
        vector<shared_ptr<HttpRemoteTask>> allTasks;
        for(auto task: this->tasks)
        {
            vector<shared_ptr<HttpRemoteTask>> nodeTasks = task.second;
            for(auto nodeTask : nodeTasks)
            {
                allTasks.push_back(nodeTask);
            }
        }
        tasksLock.unlock();
        return allTasks;

    }
    void setOutputBuffers(shared_ptr<OutputBufferSchema> schema)
    {
        this->outputBufferSchema = schema;

        map<int,int> tgm;

        for(auto t : this->taskGroups)
        {
            tgm[t.first] = t.second.size();
        }



        vector<shared_ptr<HttpRemoteTask>> allTasks = getAllTasks();
        for(int i = 0 ; i < allTasks.size() ; i++)
        {
            allTasks[i]->setOutputBuffers(this->outputBufferSchema);
        }
        this->outputBufferSeted = true;

    }

    //outputexpand 0 1 outputshrink 0 1 exchangeexpand 0 1 exchangeshrink 0 1
    bitset<4> getStageBufferSizeChangingTrend() {
        bitset<4> states("0000");
        auto allTasks = this->getAllTasks();

        int outputTrend = 0;
        int exchangeTrend = 0;
        for (auto task: allTasks) {

            if(task->isDone())
                continue;

            if (task->isTaskOutputBufferExpandTrend())
                outputTrend++;
            else if (task->isTaskOutputBufferShrinkTrend())
                outputTrend--;

            if (task->isTaskExchangeBufferExpandTrend())
                exchangeTrend++;
            else if (task->isTaskExchangeBufferShrinkTrend())
                exchangeTrend--;
        }

        if(outputTrend > 0)
            states[0] = 1;
        if(outputTrend < 0)
            states[1] = 1;
        if(exchangeTrend > 0)
            states[2] = 1;
        if(exchangeTrend < 0)
            states[3] = 1;

        return states;
    }



};


#endif //OLVP_SQLSTAGEEXECUTION_HPP
