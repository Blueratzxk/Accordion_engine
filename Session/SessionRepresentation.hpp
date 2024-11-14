//
// Created by zxk on 11/24/23.
//

#ifndef OLVP_SESSIONREPRESENTATION_HPP
#define OLVP_SESSIONREPRESENTATION_HPP

#include "nlohmann/json.hpp"
#include <string>

using namespace std;
class SessionRepresentation
{
    string queryId;
    multimap<string,vector<string>> configs;
public:
    SessionRepresentation(string queryId,multimap<string,vector<string>> configs)
    {
        this->queryId = queryId;
        this->configs = configs;

    }
    multimap<string,vector<string>> getRuntimeConfigs()
    {
        return multimap<string,vector<string>>(this->configs);
    }

    static string Serialize(SessionRepresentation sessionRepresentation)
    {
        nlohmann::json json;

        json["queryId"] = sessionRepresentation.queryId;
        json["configs"] = sessionRepresentation.configs;
        string result = json.dump();
        return result;
    }
    static shared_ptr<SessionRepresentation> Deserialize(string sessionRepresentation)
    {
        if(sessionRepresentation == "NULL")
            return NULL;
        nlohmann::json json = nlohmann::json::parse(sessionRepresentation);
        return make_shared<SessionRepresentation>(json["queryId"],json["configs"]);
    }

};



#endif //OLVP_SESSIONREPRESENTATION_HPP
