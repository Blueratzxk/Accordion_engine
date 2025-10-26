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


public:
    OperatorResponse(){}

    OperatorResponse(std::map<std::string,OPTYPE> operatorId_Operation){this->operatorId_Operation = operatorId_Operation;}

    void addOperatorId(std::string operatorId,OPTYPE operation)
    {
        operatorId_Operation[operatorId] = operation;
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


    static nlohmann::json Serialize(OperatorResponse operatorResponse)
    {
        nlohmann::json json;

        json["operatorId_Operation"] = operatorResponse.operatorId_Operation;

        return json;
    }

    static shared_ptr<OperatorResponse> Deserialize(nlohmann::json json)
    {
       // nlohmann::json json = nlohmann::json::parse(operatorResponse);


        auto result = make_shared<OperatorResponse>(json["operatorId_Operation"]);

        return  result;
    }

};




#endif //OLVP_OPERATORRESPONSE_HPP
