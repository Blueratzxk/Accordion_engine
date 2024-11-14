//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_STAGEINFO_HPP
#define OLVP_STAGEINFO_HPP


//#include "../../../Frontend/PlanNode/PlanNodeTree.hpp"
#include "../../Task/TaskInfo.hpp"
//#include "../../../Planner/Fragment.hpp"


class StageInfo
{
    string stageId;
    int taskCount;
    string stageState;
    shared_ptr<PlanFragment> planFragment;
    vector<shared_ptr<TaskInfo>> taskInfos;
public:

    StageInfo(string stageId,int taskCount,string stageState,shared_ptr<PlanFragment> planFragment,vector<shared_ptr<TaskInfo>> taskInfos){
        this->stageId = stageId;
        this->taskCount = taskCount;
        this->stageState = stageState;
        this->planFragment = planFragment;
        this->taskInfos = taskInfos;
    }
    string extractStageConcurrentMode()
    {
        if(this->planFragment->getPartitionHandle() == NULL)
            return "UNSUPPORT";
        if(this->planFragment->getPartitionHandle()->getConnectorHandle()->getHandleId().compare("SystemPartitioningHandle") == 0)
        {
            auto handle = this->planFragment->getPartitionHandle();
            if(handle->equals(*SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION")))
                return "FIXED_BROADCAST_DISTRIBUTION";
            else if(handle->equals(*SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF")))
                return "SCALED_SIMPLE_DISTRIBUTION_BUF";
            else if(handle->equals(*SystemPartitioningHandle::get("SCALED_HASH_DISTRIBUTION_BUF")))
                return "SCALED_HASH_DISTRIBUTION_BUF";
            else if(handle->equals(*SystemPartitioningHandle::get("SCALED_HASH_REDISTRIBUTION_BUF")))
                return "SCALED_HASH_REDISTRIBUTION_BUF";
            else if(handle->equals(*SystemPartitioningHandle::get("SCALED_HASH_SHUFFLE_STAGE_BUF")))
                return "SCALED_HASH_SHUFFLE_STAGE_BUF";
            else if(handle->equals(*SystemPartitioningHandle::get("SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF")))
                return "SCALED_SIMPLE_HASH_SHUFFLE_STAGE_BUF";
            else
                return "UNSUPPORT";

        }

        return "UNSUPPORT";
    }

    string ToString()
    {
        string result;
        result.append("{");
        result.append("\"stageId\":");
        result.append("\""+this->stageId+"\"");
        result.append(",");
        result.append("\"taskCount\":");
        result.append("\""+ to_string(this->taskCount)+"\"");
        result.append(",");
        result.append("\"stageState\":");
        result.append("\""+this->stageState+"\"");
        result.append(",");
        PlanNodeTreeToString planNodeTreeToString;
        string *fragmentStr = new string();
        planNodeTreeToString.Visit(this->planFragment->getRoot(),fragmentStr);

        result.append("\"stageFragment\":");
        result.append("\""+*fragmentStr+"\"");
        result.append(",");
        result.append("\"stagePartitioningStyle\":");
        result.append("\""+this->extractStageConcurrentMode()+"\"");
        result.append(",");
        result.append("\"taskInfos\":");
        result.append("[");

        for(auto taskInfo: this->taskInfos)
        {
            result.append(TaskInfo::Visualization(*taskInfo));
            result.append(",");
        }
        if(!this->taskInfos.empty())
            result.pop_back();


        result.append("]");
        result.append("}");


        return result;
    }



};

#endif //OLVP_STAGEINFO_HPP
