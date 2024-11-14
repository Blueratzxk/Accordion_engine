//
// Created by zxk on 6/4/23.
//

#ifndef OLVP_STAGETREEEXECUTIONFACTORY_HPP
#define OLVP_STAGETREEEXECUTIONFACTORY_HPP



#include "../../Planner/Fragment.hpp"
#include "StageExecutionAndScheduler.hpp"
#include "NormalStageScheduler.hpp"
#include "../../NodeCluster/NodeSelector.hpp"
#include "../../Split/SplitSourceFactory.hpp"
#include "SourcePartitionedScheduler.hpp"
#include "../../Partitioning//NodePartitioningManager.hpp"

#include "../../Session/Session.hpp"

class StageTreeExecutionFactory
{

    shared_ptr<Session> session;
public:
    StageTreeExecutionFactory(shared_ptr<Session> session){
        this->session = session;
    }

    vector<StageExecutionAndScheduler> createStageTreeExecutions(shared_ptr<SqlStageExecution> &rootStage,shared_ptr<SubPlan> planRoot)
    {
        vector<StageExecutionAndScheduler> stageTreeExecutions =  createStreamingLinkedStageExecutions(planRoot,NULL);

        shared_ptr<OutputBufferSchema> rootStageSchema = OutputBufferSchema::createInitialEmptyOutputBufferSchema(OutputBufferSchema::BROADCAST);
        rootStageSchema->changeBuffers({{"0",0}});
        rootStage = (stageTreeExecutions.end()-1)->getStageExecution();
        rootStage->setOutputBuffers(rootStageSchema);

        return stageTreeExecutions;
    }


    vector<StageExecutionAndScheduler> createStreamingLinkedStageExecutions(shared_ptr<SubPlan> planRoot,shared_ptr<SqlStageExecution> parent)
    {
        vector<StageExecutionAndScheduler> stageExecutionAndSchedulers;

        string fragmentId = planRoot->getFragment().getFragmentId();
        string stageId = planRoot->getFragment().getFragmentId();
        shared_ptr<SqlStageExecution> stageExecution = createSqlStageExecution(this->session,session->getQueryId(),session->getStageExecutionId(),atoi(stageId.c_str()),planRoot->getFragment());

        vector<shared_ptr<SqlStageExecution>> childs;

        for(auto stagePlan : planRoot->getChildren())
        {
            vector<StageExecutionAndScheduler> subTree = createStreamingLinkedStageExecutions(stagePlan,stageExecution);
            for(auto subs : subTree) {
                stageExecutionAndSchedulers.push_back(subs);
            }
            childs.push_back(subTree.back().getStageExecution());
        }


        shared_ptr<StageLinkage> linkage = make_shared<StageLinkage>(fragmentId,stageExecution,parent,childs);
        shared_ptr<StageScheduler> scheduler = createStageScheduler(stageId,planRoot,stageExecution,parent,childs);



        stageExecutionAndSchedulers.push_back(StageExecutionAndScheduler(stageExecution,linkage,scheduler));

        return stageExecutionAndSchedulers;
    }

    shared_ptr<SqlStageExecution> createSqlStageExecution(shared_ptr<Session> session,string queryId,int stageExecutionId,int stageId,PlanFragment fragment)
    {
        return make_shared<SqlStageExecution>(session,queryId,stageExecutionId,stageId, make_shared<PlanFragment>(fragment.getRoot(),fragment.getFragmentId(),fragment.getPartitionScheme(),fragment.getPartitionHandle()));
    }

    shared_ptr<StageScheduler> createStageScheduler(string stageId,shared_ptr<SubPlan> plan,shared_ptr<SqlStageExecution> stageExecution,
                                                    shared_ptr<SqlStageExecution> parentStageExecution,vector<shared_ptr<SqlStageExecution>> childs)
    {

        shared_ptr<StageScheduler> scheduler;

        SplitSourceFactory splitSourceFactory;
        map<string,vector<shared_ptr<Split>>> dataSplitSources = splitSourceFactory.createSplitSources(plan->getFragment());

        NodeSelector selector;
        //vector<clusterNode> node = selector.getRandomNode();

        NodePartitioningManager nodePartitioningManager;



        //   if(stageId.compare("3") == 0)
        //       node = selector.getRandomNodes(2);

        if(!dataSplitSources.empty())
        {
            scheduler = make_shared<SourcePartitionedScheduler>(stageExecution,dataSplitSources);
        }
        else {

            auto np = nodePartitioningManager.getNodePartitioningMap(plan->getFragment().getPartitionHandle());
            vector<shared_ptr<ClusterNode>> node = np.getPartitionToNode();

            auto handle = plan->getFragment().getPartitionHandle();
            if(handle->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0)
            {
                if(static_pointer_cast<SystemPartitioningHandle>((handle)->getConnectorHandle())->partitioningType == SystemPartitioningHandle::HASH_SCALED) {

                }
            }
            if(node.empty())
                scheduler = NULL;
            else
                scheduler = make_shared<NormalStageScheduler>(stageExecution, node,np.getNodeGroups());
        }

        return scheduler;
    }
};



#endif //OLVP_STAGETREEEXECUTIONFACTORY_HPP
