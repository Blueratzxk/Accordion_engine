//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_TASKID_HPP
#define OLVP_TASKID_HPP

#include "StageExecutionId.hpp"

#include "nlohmann/json.hpp"


//Taskid = QueryId.StageExecutionId.StageId.id
class TaskId
{

    StageExecutionId stageExecutionId;
    int id;

public:
    TaskId(){

    }
    TaskId(string queryId,int stageExecutionId,int stageId,int id)
    {
        this->stageExecutionId = StageExecutionId(StageId(QueryId(queryId),stageId),stageExecutionId);
        this->id = id;
    }

    TaskId(StageExecutionId stageExecutionId,int id)
    {
        this->stageExecutionId = stageExecutionId;
        this->id = id;
    }

    StageExecutionId getStageExecutionId()
    {
        return this->stageExecutionId;
    }


    QueryId getQueryId()
    {
        return this->stageExecutionId.getStageId().getQueryId();
    }


    int getId()
    {
        return this->id;
    }

    StageId getStageId()
    {
        return this->stageExecutionId.getStageId();
    }

    bool operator<(const TaskId &p) const //注意这里的两个const
    {
        return (id < p.id) || (stageExecutionId < p.stageExecutionId);
    }
    bool operator==(const TaskId &p) const //注意这里的两个const
    {
        return (id == p.id) || (stageExecutionId == p.stageExecutionId);
    }
    string ToString()
    {
        string result;
        string queryId = this->getQueryId().getId();
        string stageExecutionId = to_string(this->getStageExecutionId().getId());
        string stagId = to_string(this->getStageId().getId());
        string tid = to_string(this->id);
        result = queryId+"$"+stageExecutionId+"$"+stagId+"$"+tid;

        return result;
    }
    void splitStringTaskId(const std::string& s, std::vector<std::string>& tokens, const std::string& delimiters = " ")
    {
        std::string::size_type lastPos = s.find_first_not_of(delimiters, 0);
        std::string::size_type pos = s.find_first_of(delimiters, lastPos);
        while (std::string::npos != pos || std::string::npos != lastPos) {
            tokens.push_back(s.substr(lastPos, pos - lastPos));
            lastPos = s.find_first_not_of(delimiters, pos);
            pos = s.find_first_of(delimiters,lastPos);
        }
    }
    shared_ptr<TaskId> StringToObject(string taskId)
    {
        vector<string> ids;
        splitStringTaskId(taskId,ids,"$");

        if(ids.size() < 4)
            return NULL;

        shared_ptr<TaskId> result = make_shared<TaskId>(ids[0],atoi(ids[1].c_str()),atoi(ids[2].c_str()),atoi(ids[3].c_str()));
        return result;
    }




    static string Serialize(TaskId taskId)
    {
        nlohmann::json json;
        json["id"] = taskId.id;
        json["stageExecutionId"] = StageExecutionId::Serialize(taskId.stageExecutionId);

        string result = json.dump();
        return result;
    }
    static shared_ptr<TaskId> Deserialize(string taskId)
    {
        nlohmann::json json = nlohmann::json::parse(taskId);
        return make_shared<TaskId>(*StageExecutionId::Deserialize(json["stageExecutionId"]),json["id"]);
    }






};


#endif //OLVP_TASKID_HPP
