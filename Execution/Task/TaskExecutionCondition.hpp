//
// Created by zxk on 4/25/25.
//

#ifndef OLVP_TASKEXECUTIONCONDITION_HPP
#define OLVP_TASKEXECUTIONCONDITION_HPP


class TaskExecutionCondition
{
    string conditionType;
    string componentId;
public:
    TaskExecutionCondition(string conditionType,string componentId){
        this->componentId = componentId;
        this->conditionType = conditionType;
    }

    string getConditionType(){return this->conditionType;}
    string getComponentId(){return this->componentId;}

    static string Serialize(shared_ptr<TaskExecutionCondition> taskExecutionCondition)
    {
        nlohmann::json json;

        json["conditionType"] = taskExecutionCondition->conditionType;
        json["componentId"] = taskExecutionCondition->componentId;

        string result = json.dump();

        return result;
    }

    static shared_ptr<TaskExecutionCondition> Deserialize(string taskExecutionCondition)
    {
        if(taskExecutionCondition == "NULL")
            return make_shared<TaskExecutionCondition>("","");
        nlohmann::json json = nlohmann::json::parse(taskExecutionCondition);
        return make_shared<TaskExecutionCondition>(json["conditionType"],json["componentId"]);
    }




};


#endif //OLVP_TASKEXECUTIONCONDITION_HPP
