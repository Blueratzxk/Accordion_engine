//
// Created by zxk on 10/25/25.
//

#ifndef OLVP_OPERATORRESPONSE_HPP
#define OLVP_OPERATORRESPONSE_HPP

class OperatorResponse
{
public:
    enum OPTYPE
    {
        MIGRATION,
        CLOSE,
        NOT_BUILD_COMPLETE
    };

private:
    std::map<std::string,OPTYPE> operatorId_Operation;
    std::map<std::string,vector<string>> operatorId_Parameters;
    long migrationDataSize = 0;


public:
    OperatorResponse(){}

    OperatorResponse(std::map<std::string,OPTYPE> operatorId_Operation, long migrationDataSize,std::map<std::string,vector<string>> operatorId_Parameters) {
        this->operatorId_Operation = operatorId_Operation;
        this->migrationDataSize = migrationDataSize;
        this->operatorId_Parameters = operatorId_Parameters;
    }

    void addOperatorId(std::string operatorId,OPTYPE operation)
    {
        operatorId_Operation[operatorId] = operation;
    }

    void addOperatorId_Parameters(std::string operatorId,vector<string> parameters)
    {
        operatorId_Parameters[operatorId] = parameters;
    }

    vector<string> getParametersByOperatorId(std::string operatorId) {
        return this->operatorId_Parameters[operatorId];
    }


    std::set<std::string> getOperatorIds(){
        std::set<std::string> ids;
        for(auto id : operatorId_Operation)
            ids.insert(id.first);

        return ids;
    }

    std::set<std::string> getOperatorIdsNeedMigration(){
        std::set<std::string> ids;
        for(auto id : operatorId_Operation) {
            if(id.second == MIGRATION || id.second == NOT_BUILD_COMPLETE)
                ids.insert(id.first);
        }

        return ids;
    }

    std::set<std::string> getOperatorIdsNeedClose(){
        std::set<std::string> ids;
        for(auto id : operatorId_Operation) {
            if(id.second == CLOSE)
                ids.insert(id.first);
        }

        return ids;
    }

    bool hasBuildCompleteState()
    {
        for(auto op_op : this->operatorId_Operation)
        {
            if(op_op.second == NOT_BUILD_COMPLETE)
                return true;
        }

        return false;
    }

    void setMigrationDataSize(long size) {
        this->migrationDataSize = size;
    }

    long getMigrationDataSize() {
        return this->migrationDataSize;
    }


    static nlohmann::json Serialize(OperatorResponse operatorResponse) {
        nlohmann::json json;

        json["operatorId_Operation"] = operatorResponse.operatorId_Operation;
        json["migrationDataSize"] = operatorResponse.migrationDataSize;
        json["operatorId_Parameters"] = operatorResponse.operatorId_Parameters;

        return json;
    }

    static shared_ptr<OperatorResponse> Deserialize(nlohmann::json json)
    {
       // nlohmann::json json = nlohmann::json::parse(operatorResponse);


        auto result = make_shared<OperatorResponse>(json["operatorId_Operation"],json["migrationDataSize"],json["operatorId_Parameters"]);

        return  result;
    }

};




#endif //OLVP_OPERATORRESPONSE_HPP
