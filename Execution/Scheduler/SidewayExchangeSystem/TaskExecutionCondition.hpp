//
// Created by zxk on 4/25/25.
//

#ifndef OLVP_TASKEXECUTIONCONDITION_HPP
#define OLVP_TASKEXECUTIONCONDITION_HPP


class MigratedBufferAddress
{
    vector<string> TaskId;
    vector<string> IP;
    vector<string> Port;
    vector<string> BufferId;

    int numAddresses = 0;
public:
    MigratedBufferAddress()
    {
    }


    MigratedBufferAddress(vector<string> TaskId, vector<string> IP, vector<string> Port,vector<string> BufferId)
    {
        this->TaskId = TaskId;
        this->IP = IP;
        this->Port = Port;
        this->BufferId = BufferId;

        this->numAddresses = TaskId.size();
    }

    int getNumAddresses()
    {
        return this->numAddresses;
    }

    void addMigratedBufferAddress(string TaskId,string IP,string Port,string BufferId)
    {
        this->TaskId.push_back(TaskId);
        this->IP.push_back(IP);
        this->Port.push_back(Port);
        this->BufferId.push_back(BufferId);

        this->numAddresses++;
    }


    string getTaskId(int index){return TaskId[index];}
    string getIP(int index){return IP[index];}
    string getPort(int index){return Port[index];}
    string getBufferId(int index){return BufferId[index];}

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

    map<string,set<string>> operator_Type_Id_Map;

public:
    MigratedOperators()
    {
        this->TaskId = "";
        this->IP = "";
        this->Port = "";

    }

    MigratedOperators(set<string> sourceTypes,map<string,set<string>> operator_Type_Id_Map,set<string> operatorsNeedInterTaskExchange,string TaskId, string IP, string Port)
    {
        this->TaskId = TaskId;
        this->IP = IP;
        this->Port = Port;
        this->sourceTypes = sourceTypes;
        this->operatorsNeedInterTaskExchange = operatorsNeedInterTaskExchange;
        this->operator_Type_Id_Map = operator_Type_Id_Map;
    }

    string getTaskId(){return TaskId;}
    string getIP(){return IP;}
    string getPort(){return Port;}

    set<string> getSourceTypes(){return this->sourceTypes;}
    set<string> getOperatorsNeedInterTaskExchange(){return this->operatorsNeedInterTaskExchange;}
    map<string,set<string>> getOperator_Type_Id_Map(){return this->operator_Type_Id_Map;}

    static string Serialize(MigratedOperators migratedOperators)
    {
        nlohmann::json json;

        json["TaskId"] = migratedOperators.TaskId;
        json["IP"] = migratedOperators.IP;
        json["Port"] = migratedOperators.Port;
        json["SourceTypes"] = migratedOperators.sourceTypes;
        json["operatorsNeedInterTaskExchange"] = migratedOperators.operatorsNeedInterTaskExchange;
        json["operator_Type_Id_Map"] = migratedOperators.operator_Type_Id_Map;
        string result = json.dump();

        return result;
    }

    static MigratedOperators Deserialize(string migratedBufferAddress)
    {
        nlohmann::json json = nlohmann::json::parse(migratedBufferAddress);
        return MigratedOperators(json["SourceTypes"],json["operator_Type_Id_Map"],json["operatorsNeedInterTaskExchange"],json["TaskId"],json["IP"],json["Port"]);
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
        HETERO_TASK_SCHEDULE,
        NO_CONDITION
    };

private:
    ConditionType conditionType;

    MigratedBufferAddress migratedBufferAddress;
    MigratedOperators migratedOperators;

    string extension;

public:

    TaskExecutionCondition(){
        this->conditionType = NO_CONDITION;
    }

    TaskExecutionCondition(ConditionType conditionType,MigratedBufferAddress migratedBufferAddress,MigratedOperators migratedOperators,string extension){
        this->migratedBufferAddress = migratedBufferAddress;
        this->migratedOperators = migratedOperators;
        this->conditionType = conditionType;
        this->extension = extension;

    }

    TaskExecutionCondition(ConditionType conditionType,MigratedOperators migratedOperators){

        this->conditionType = conditionType;
        this->migratedOperators = migratedOperators;

    }

    TaskExecutionCondition(ConditionType conditionType, MigratedBufferAddress migratedBufferAddress){
        this->migratedBufferAddress = migratedBufferAddress;
        this->conditionType = conditionType;
    }

    TaskExecutionCondition(ConditionType conditionType,string extension){
        this->conditionType = conditionType;
        this->extension = extension;
    }

    MigratedBufferAddress getMigratedBufferAddress(){return migratedBufferAddress;}

    MigratedOperators getMigratedOperators(){return migratedOperators;}

    ConditionType getConditionType(){return this->conditionType;}

    string getExtension(){return this->extension;}

    static string Serialize(shared_ptr<TaskExecutionCondition> taskExecutionCondition)
    {
        nlohmann::json json;

        json["conditionType"] = taskExecutionCondition->conditionType;
        json["migratedBufferAddress"] = MigratedBufferAddress::Serialize(taskExecutionCondition->migratedBufferAddress);
        json["migratedOperators"] = MigratedOperators::Serialize(taskExecutionCondition->migratedOperators);
        json["extension"] = taskExecutionCondition->extension;
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

        return make_shared<TaskExecutionCondition>(json["conditionType"],migratedBufferAddress,migratedOperators,json["extension"]);
    }




};


#endif //OLVP_TASKEXECUTIONCONDITION_HPP
