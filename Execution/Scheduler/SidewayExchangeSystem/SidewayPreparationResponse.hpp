//
// Created by zxk on 10/25/25.
//

#ifndef OLVP_SIDEWAYPREPARATIONRESPONSE_HPP
#define OLVP_SIDEWAYPREPARATIONRESPONSE_HPP

#include "OperatorResponse.hpp"
#include <string>
#include <map>
#include <memory>
#include <string>
#include <set>
class SidewayPreparationResponse
{

    std::map<std::string,std::set<std::shared_ptr<OperatorResponse>>> sourceToResponses;

public:

    SidewayPreparationResponse(){}

    SidewayPreparationResponse(std::map<std::string,std::set<std::shared_ptr<OperatorResponse>>> sourceToResponses){this->sourceToResponses = sourceToResponses;}

    void addSourceResponse(std::string sourceType,std::shared_ptr<OperatorResponse> response)
    {
        if(!sourceToResponses.contains(sourceType)) {
            sourceToResponses[sourceType] = {};
            sourceToResponses[sourceType].insert(response);
        }
        else
            sourceToResponses[sourceType].insert(response);
    }

    bool isSourceTypeHasNotBuildCompleteStateAndHasOperatorId(string sourceType,string operatorId)
    {
        for(auto sR : sourceToResponses)
        {
            if(sR.first == sourceType)
            {
                for(auto res : sR.second)
                {
                    if(res->hasBuildCompleteState())
                    {
                        for(auto opID : res->getOperatorIds())
                        {
                            if(opID == operatorId)
                                return true;
                        }
                    }
                }

            }

        }

        return false;
    }

    set<string> getOperatorIdsNeedMigrationByOperatorType(string type) {
        set<string> ids;

        if (!sourceToResponses.contains(type))
            return ids;

        for (auto id: sourceToResponses[type]) {
            auto idSet = id->getOperatorIdsNeedMigration();
            for (auto id: idSet)
                ids.insert(id);
        }

        return ids;
    }


    map<string,vector<string>> getOperatorIdsParametersNeedMigrationByOperatorType(string type) {

       map<string,vector<string>> ids;

        if (!sourceToResponses.contains(type))
            return ids;

        for (auto id: sourceToResponses[type]) {
            auto idSet = id->getOperatorIdsNeedMigration();
            for (auto idStr: idSet)
                ids[idStr] = id->getParametersByOperatorId(idStr);
        }

        return ids;
    }


    set<string> getOperatorIdsNeedCloseByOperatorType(string type) {
        set<string> ids;

        if(!sourceToResponses.contains(type))
            return ids;

        for (auto id: sourceToResponses[type]) {
            auto idSet = id->getOperatorIdsNeedClose();
            for (auto id: idSet)
                ids.insert(id);
        }

        return ids;
    }

    set<string> getNotBuildCompleteStateSources()
    {
        set<string> allSources;
        for(auto sR : sourceToResponses)
        {
            for(auto res : sR.second)
                if(res->hasBuildCompleteState())
                    allSources.insert(sR.first);
        }
        return allSources;
    }

    bool hasNotBuildCompleteState()
    {
        for(auto sR : sourceToResponses)
        {
            for(auto res : sR.second)
                if(res->hasBuildCompleteState())
                return true;
        }

        return false;
    }

    long getTotalMigrationBytes() {

        long totalMigrationBytes = 0;
        for (auto res : sourceToResponses) {
            for (auto reponses : res.second) {
                totalMigrationBytes += reponses->getMigrationDataSize();
            }
        }
        return totalMigrationBytes;
    }



    static string Serialize(SidewayPreparationResponse sidewayPreparationResponse)
    {
        nlohmann::json sourceToResponsesJson;


        for(auto sourceResponses : sidewayPreparationResponse.sourceToResponses)
        {
            nlohmann::json set;
            for(auto res: sourceResponses.second)
                set.push_back(OperatorResponse::Serialize(*res));
            sourceToResponsesJson[sourceResponses.first] = set;
        }


        string result = sourceToResponsesJson.dump();
        return result;
    }

    static shared_ptr<SidewayPreparationResponse> Deserialize(string sidewayPreparationResponse)
    {
        nlohmann::json json = nlohmann::json::parse(sidewayPreparationResponse);


        std::map<std::string,std::set<std::shared_ptr<OperatorResponse>>> sourceToResponses;
        for(auto &item : json.items())
        {
            nlohmann::json responses = item.value();

            std::set<std::shared_ptr<OperatorResponse>> opResponses;
            for(auto &res : responses)
                opResponses.insert(OperatorResponse::Deserialize(res));

            sourceToResponses[item.key()] = opResponses;
        }


        auto result = make_shared<SidewayPreparationResponse>(sourceToResponses);

        return  result;
    }



};




#endif //OLVP_SIDEWAYPREPARATIONRESPONSE_HPP
