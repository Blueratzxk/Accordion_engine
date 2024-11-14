//
// Created by zxk on 6/2/23.
//

#ifndef OLVP_LOGICALPIPELINERUNNER_HPP
#define OLVP_LOGICALPIPELINERUNNER_HPP

#include "SplitRunner.hpp"
#include "../../Planner/LocalPlanner/LogicalPipelineFactory.hpp"
#include "../../Pipeline/PhysicalPipeline.hpp"
#include "ScheduledSplit.hpp"
#include "../../Operators/TableScanOperator.hpp"
#include "../../Operators/RemoteSourceOperator.hpp"
#include "Context/DriverContext.h"
#include "Context/PipelineContext.h"

class LogicalPipelineRunner : public SplitRunner
{
    std::shared_ptr<LogicalPipeline> logicalPipeline = NULL;
    std::shared_ptr<ScheduledSplit> scheduledSplit = NULL;
    set<std::shared_ptr<Split>> scheduledSplits;
    shared_ptr<vector<shared_ptr<Operator>>> driver = NULL;
    shared_ptr<DriverContext> driverContext;

    shared_ptr<Session> session;
public:
    LogicalPipelineRunner(shared_ptr<Session> session,shared_ptr<DriverContext> driverContext,std::shared_ptr<LogicalPipeline> logicalPipeline,std::shared_ptr<ScheduledSplit> scheduledSplit){
        this->driverContext = driverContext;
        this->scheduledSplit = scheduledSplit;
        this->logicalPipeline = logicalPipeline;
        this->session = session;
    }
    LogicalPipelineRunner(shared_ptr<Session> session,shared_ptr<DriverContext> driverContext,std::shared_ptr<LogicalPipeline> logicalPipeline,set<std::shared_ptr<Split>> scheduledSplits){
        this->scheduledSplits = scheduledSplits;
        this->logicalPipeline = logicalPipeline;
        this->driverContext = driverContext;
        this->session = session;
    }



    void ProcessFor() override
    {
        if(driver == NULL) {
            this->driver = logicalPipeline->getPhysicalPipelineReturnAddr(this->driverContext);
            this->driverContext->setDriver(this->driver);

            this->driverContext->releaseCachedLocations();

        }

        if(this->scheduledSplit != NULL) {


            for(int i = 0 ; i < (*this->driver).size() ; i++)
            {
                if((*this->driver)[i]->getOperatorId() == "TableScanOperator")
                {
                    static_pointer_cast<TableScanOperator>((*this->driver)[i])->addSplits(this->session,*(this->scheduledSplit->getSplit()));
                    this->scheduledSplit = NULL;
                    break;
                }

            }

        }
        if(this->scheduledSplits.size() > 0)
        {
            for(int i = 0 ; i < (*this->driver).size() ; i++) {

                if ((*this->driver)[i]->getOperatorId() == "RemoteSourceOperator") {

                    static_pointer_cast<RemoteSourceOperator>((*this->driver)[i])->addSources(this->scheduledSplits);
                    break;
                }
            }
        }

        PhysicalPipeline::runPipeline(this->driver,this->driverContext);
        this->driverContext->getPipelineContext().lock()->finishDriverContext(this->driverContext);

    }


};




class LogicalPipelineRunnerFactory
{
    std::shared_ptr<LogicalPipeline> logicalPipeline;
    shared_ptr<PipelineContext> pipelineContext;
    shared_ptr<Session> session;

public:
    LogicalPipelineRunnerFactory(shared_ptr<Session> session,shared_ptr<PipelineContext> pipelineContext,std::shared_ptr<LogicalPipeline> logicalPipeline)
    {
        this->logicalPipeline = logicalPipeline;
        this->pipelineContext = pipelineContext;
        this->session = session;
    }

    std::shared_ptr<LogicalPipeline> getLogicalPipeline()
    {
        return this->logicalPipeline;
    }

    std::shared_ptr<LogicalPipelineRunner> createLogicalPipelineRunner(std::shared_ptr<ScheduledSplit> scheduledSplit)
    {
        shared_ptr<DriverContext> driverContext = this->pipelineContext->addDriverContext();
        return std::make_shared<LogicalPipelineRunner>(this->session,driverContext,this->logicalPipeline,scheduledSplit);
    }

    std::shared_ptr<LogicalPipelineRunner> createLogicalPipelineRunner(set<std::shared_ptr<Split>> Splits)
    {
        shared_ptr<DriverContext> driverContext = this->pipelineContext->addDriverContext();
        return std::make_shared<LogicalPipelineRunner>(this->session,driverContext,this->logicalPipeline,Splits);
    }


};





#endif //OLVP_LOGICALPIPELINERUNNER_HPP
