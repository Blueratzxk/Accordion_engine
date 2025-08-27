//
// Created by zxk on 4/25/25.
//

#ifndef OLVP_TASKEXECUTIONCONDITION_HPP
#define OLVP_TASKEXECUTIONCONDITION_HPP


class MigratedBufferAddress
{
    string TaskId;
    string IP;
    string Port;
    string BufferId;
public:
    MigratedBufferAddress()
    {
        this->TaskId = "";
        this->IP = "";
        this->Port = "";
        this->BufferId = "";
    }

    MigratedBufferAddress(string TaskId, string IP, string Port, string BufferId)
    {
        this->TaskId = TaskId;
        this->IP = IP;
        this->Port = Port;
        this->BufferId = BufferId;
    }

    string getTaskId(){return TaskId;}
    string getIP(){return IP;}
    string getPort(){return Port;}
    string getBufferId(){return BufferId;}

    static string Serialize(MigratedBufferAddress migratedBufferAddress)
    {
        nlohmann::json json;

        json["TaskId"] = migratedBufferAddress.TaskId;
        json["IP"] = migratedBufferAddress.IP;
        json["Port"] = migratedBufferAddress.Port;
        json["BufferId"] = migratedBufferAddress.BufferId;



        string result = json.dump();

        return result;
    }

    static MigratedBufferAddress Deserialize(string migratedBufferAddress)
    {
        nlohmann::json json = nlohmann::json::parse(migratedBufferAddress);
        return MigratedBufferAddress(json["TaskId"],json["IP"],json["Port"],json["BufferId"]);
    }


};


class MigratedOperators
{
    set<string> sourceTypes;
    set<string> operatorsNeedInterTaskExchange;
    string TaskId;
    string IP;
    string Port;

public:
    MigratedOperators()
    {
        this->TaskId = "";
        this->IP = "";
        this->Port = "";

    }

    MigratedOperators(set<string> sourceTypes,set<string> operatorsNeedInterTaskExchange,string TaskId, string IP, string Port)
    {
        this->TaskId = TaskId;
        this->IP = IP;
        this->Port = Port;
        this->sourceTypes = sourceTypes;
        this->operatorsNeedInterTaskExchange = operatorsNeedInterTaskExchange;

    }

    string getTaskId(){return TaskId;}
    string getIP(){return IP;}
    string getPort(){return Port;}

    set<string> getSourceTypes(){return this->sourceTypes;}
    set<string> getOperatorsNeedInterTaskExchange(){return this->operatorsNeedInterTaskExchange;}

    static string Serialize(MigratedOperators migratedOperators)
    {
        nlohmann::json json;

        json["TaskId"] = migratedOperators.TaskId;
        json["IP"] = migratedOperators.IP;
        json["Port"] = migratedOperators.Port;
        json["SourceTypes"] = migratedOperators.sourceTypes;
        json["operatorsNeedInterTaskExchange"] = migratedOperators.operatorsNeedInterTaskExchange;

        string result = json.dump();

        return result;
    }

    static MigratedOperators Deserialize(string migratedBufferAddress)
    {
        nlohmann::json json = nlohmann::json::parse(migratedBufferAddress);
        return MigratedOperators(json["SourceTypes"],json["operatorsNeedInterTaskExchange"],json["TaskId"],json["IP"],json["Port"]);
    }


};

class ConditionExecutionResult
{
    set<string> operatorIds;
public:
    ConditionExecutionResult(){}

    void addOperatorId(string operatorId){operatorIds.insert(operatorId);}

    set<string> getOperatorIds(){return operatorIds;}
};

class TaskExecutionCondition
{
public:
  enum ConditionType {
        OPERATOR_MIGRATION,
        BUFFER_MIGRATION,
        NO_CONDITION
    };

private:
    ConditionType conditionType;

    MigratedBufferAddress migratedBufferAddress;
    MigratedOperators migratedOperators;

    ConditionExecutionResult conditionExecutionResults;
public:

    TaskExecutionCondition(){
        this->conditionType = NO_CONDITION;
    }

    TaskExecutionCondition(ConditionType conditionType,MigratedBufferAddress migratedBufferAddress,MigratedOperators migratedOperators){
        this->migratedBufferAddress = migratedBufferAddress;
        this->migratedOperators = migratedOperators;
        this->conditionType = conditionType;

    }

    TaskExecutionCondition(ConditionType conditionType,MigratedOperators migratedOperators){

        this->conditionType = conditionType;
        this->migratedOperators = migratedOperators;

    }

    TaskExecutionCondition(ConditionType conditionType, MigratedBufferAddress migratedBufferAddress){
        this->migratedBufferAddress = migratedBufferAddress;
        this->conditionType = conditionType;
    }

    void addOperatorId(string id){
        this->conditionExecutionResults.addOperatorId(id);
    }

    set<string> getTargetOperatorIds()
    {
        return this->conditionExecutionResults.getOperatorIds();
    }


    MigratedBufferAddress getMigratedBufferAddress(){return migratedBufferAddress;}

    MigratedOperators getMigratedOperators(){return migratedOperators;}

    ConditionType getConditionType(){return this->conditionType;}

    static string Serialize(shared_ptr<TaskExecutionCondition> taskExecutionCondition)
    {
        nlohmann::json json;

        json["conditionType"] = taskExecutionCondition->conditionType;
        json["migratedBufferAddress"] = MigratedBufferAddress::Serialize(taskExecutionCondition->migratedBufferAddress);
        json["migratedOperators"] = MigratedOperators::Serialize(taskExecutionCondition->migratedOperators);

        string result = json.dump();

        return result;
    }

    static shared_ptr<TaskExecutionCondition> Deserialize(string taskExecutionCondition)
    {
        if(taskExecutionCondition == "NULL")
            return make_shared<TaskExecutionCondition>();
        nlohmann::json json = nlohmann::json::parse(taskExecutionCondition);

        MigratedBufferAddress migratedBufferAddress = MigratedBufferAddress::Deserialize(json["migratedBufferAddress"]);
        MigratedOperators migratedOperators = MigratedOperators::Deserialize(json["migratedOperators"]);

        return make_shared<TaskExecutionCondition>(json["conditionType"],migratedBufferAddress,migratedOperators);
    }




};


#endif //OLVP_TASKEXECUTIONCONDITION_HPP
