//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_LOGICALPIPELINEFACTORY_HPP
#define OLVP_LOGICALPIPELINEFACTORY_HPP



#include "../../Operators/LogicalOperators/LogicalOperator.hpp"
#include <string>
#include <vector>
#include "../../Frontend/PlanNode/PlanNodeId.hpp"
#include "spdlog/spdlog.h"
using namespace std;



#define PIPELINE_TYPE_SINK 1
#define PIPELINE_TYPE_SOURCE 2
#define PIPELINE_TYPE_TABLESCAN 3
#define PIPELINE_TYPE_OUTPUT 4
#define PIPELINE_TYPE_REMOTESOURCE 5
#define PIPELINE_TYPE_BUILD 6
#define PIPELINE_TYPE_PROBE 7

#include "PipelineId.hpp"
class LogicalPipeline
{
    PipelineId pipelineId;
    vector<int> pipelineType;
    vector<std::shared_ptr<LogicalOperator>> pipelineTemplate;
    PlanNodeId sourceId;

public:
    LogicalPipeline(vector<std::shared_ptr<LogicalOperator>> pipelineTemplate)
    {
        this->pipelineTemplate = pipelineTemplate;
    }
    LogicalPipeline(string pipelineId,vector<std::shared_ptr<LogicalOperator>> pipelineTemplate)
    {
        this->pipelineId = PipelineId(pipelineId);
        this->pipelineTemplate = pipelineTemplate;
    }
    void setSourceId(string id)
    {
        this->sourceId = PlanNodeId(id);
    }
    bool hasSourceId()
    {
        if(this->sourceId.get().compare("NULL") == 0)
            return false;
        else
            return true;
    }
    PlanNodeId getSourceId()
    {
        return this->sourceId;
    }

    std::shared_ptr<LogicalPipeline> getNewLogicalPipeline(LogicalPipeline lp)
    {
        return std::make_shared<LogicalPipeline>(lp.pipelineId.get(),lp.pipelineType,lp.pipelineTemplate);
    }
    string getPipelineTypesString()
    {
        vector<string> typeStrings;

        for(auto type : this->pipelineType)
        {
            switch(type){
                case PIPELINE_TYPE_SINK: typeStrings.push_back("PIPELINE_TYPE_SINK");break;
                case PIPELINE_TYPE_SOURCE:typeStrings.push_back("PIPELINE_TYPE_SOURCE"); break;
                case PIPELINE_TYPE_TABLESCAN: typeStrings.push_back("PIPELINE_TYPE_TABLESCAN");break;
                case PIPELINE_TYPE_OUTPUT:typeStrings.push_back("PIPELINE_TYPE_OUTPUT"); break;
                case PIPELINE_TYPE_REMOTESOURCE: typeStrings.push_back("PIPELINE_TYPE_REMOTESOURCE");break;
                case PIPELINE_TYPE_BUILD: typeStrings.push_back("PIPELINE_TYPE_BUILD");break;
                case PIPELINE_TYPE_PROBE: typeStrings.push_back("PIPELINE_TYPE_PROBE");break;
                default:
                    typeStrings.push_back("UNKNOWN PIPELINE TYPE!");
                    break;
            }

        }

        string result;

        for(auto str: typeStrings)
        {
            result += str;
            result += "&";
        }
        result.pop_back();
        return result;

    }
    vector<int> getPipelineTypes()
    {
        return this->pipelineType;
    }
    string getPipelineId()
    {
        return this->pipelineId.get();
    }
    void renamePipelineId(string name)
    {
        this->pipelineId = name;
    }

    LogicalPipeline(string pipelineId,vector<int> pipelineType,vector<std::shared_ptr<LogicalOperator>> pipelineTemplate)
    {
        this->pipelineId = pipelineId;
        this->pipelineTemplate = pipelineTemplate;
        this->pipelineType = pipelineType;
    }
    vector<std::shared_ptr<LogicalOperator>> getLogicalPipelines()
    {
        return this->pipelineTemplate;
    }
    vector<string> getPipelineOperatorTypes()
    {
        vector<string> pipelineIds;
        for(int i = 0 ; i < this->pipelineTemplate.size() ; i++)
            pipelineIds.push_back(this->pipelineTemplate[i]->getTypeId());
        return pipelineIds;

    }

    vector<std::shared_ptr<Operator>> getPhysicalPipeline(shared_ptr<DriverContext> driverContext)
    {
        vector<std::shared_ptr<Operator>> physicalOperators;

        for (int i = 0; i < pipelineTemplate.size(); i++)
        {
            physicalOperators.push_back(pipelineTemplate[i]->getOperator(driverContext));
        }
        return physicalOperators;
    }
    vector<std::shared_ptr<Operator>> getPhysicalPipelineFromNonType(shared_ptr<DriverContext> driverContext)
    {
        vector<std::shared_ptr<Operator>> physicalOperators;

        for (int i = 0; i < pipelineTemplate.size(); i++)
        {
            std::shared_ptr<void> alloc = NULL;
            alloc = std::static_pointer_cast<Operator>(pipelineTemplate[i]->getOperatorNonType(driverContext));
            if (alloc == NULL)
            {
                spdlog::critical("Physical operator creating failed!");
                physicalOperators.clear();
                return physicalOperators;
            }
            physicalOperators.push_back(std::static_pointer_cast<Operator>(alloc));
        }
        return physicalOperators;
    }


    vector<std::shared_ptr<void>>  getPhysicalPipelineNonType(shared_ptr<DriverContext> driverContext)
    {
        vector<std::shared_ptr<void>> physicalOperators;

        for (int i = 0; i < pipelineTemplate.size(); i++)
        {
            std::shared_ptr<void> alloc = NULL;
            alloc = std::static_pointer_cast<Operator>(pipelineTemplate[i]->getOperatorNonType(driverContext));
            if (alloc == NULL)
            {
                spdlog::critical("Physical operator creating failed!");
                physicalOperators.clear();
                return physicalOperators;
            }
            physicalOperators.push_back(alloc);
        }
        return physicalOperators;
    }

    std::shared_ptr<vector<std::shared_ptr<void>>> getPhysicalPipelineNonTypeReturnAddr(shared_ptr<DriverContext> driverContext)
    {
        std::shared_ptr<vector<std::shared_ptr<void>>> physicalOperators = std::make_shared<vector<std::shared_ptr<void>>>();

        for (int i = 0; i < pipelineTemplate.size(); i++)
        {
            std::shared_ptr<void> alloc = NULL;
            alloc = std::static_pointer_cast<Operator>(pipelineTemplate[i]->getOperatorNonType(driverContext));
            if (alloc == NULL)
            {
                spdlog::critical("Physical operator creating failed!");
                physicalOperators->clear();
                return physicalOperators;
            }
            physicalOperators->push_back(alloc);
        }
        return physicalOperators;
    }
    std::shared_ptr<vector<std::shared_ptr<Operator>>> getPhysicalPipelineReturnAddr(shared_ptr<DriverContext> driverContext)
    {
        std::shared_ptr<vector<std::shared_ptr<Operator>>> physicalOperators = std::make_shared<vector<std::shared_ptr<Operator>>>();

        for (int i = 0; i < pipelineTemplate.size(); i++)
        {
            std::shared_ptr<Operator> alloc = NULL;
            alloc = std::static_pointer_cast<Operator>(pipelineTemplate[i]->getOperatorNonType(driverContext));
            if (alloc == NULL)
            {
                spdlog::critical("Physical operator creating failed!");
                physicalOperators->clear();
                return physicalOperators;
            }
            physicalOperators->push_back(alloc);
        }
        return physicalOperators;
    }



};


#endif //OLVP_LOGICALPIPELINEFACTORY_HPP
