//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_QUERYID_HPP
#define OLVP_QUERYID_HPP

#include <iostream>
#include "nlohmann/json.hpp"
using namespace std;
class QueryId
{
private:
    string id;

public:
    QueryId(){

    }
    QueryId(string id){
        this->id = id;
    }
    string getId()
    {
        return this->id;
    }

    bool operator<(const QueryId &p) const //注意这里的两个const
    {
        return (id < p.id);
    }
    bool operator==(const QueryId &p) const //注意这里的两个const
    {
        return (id == p.id);
    }

    static string Serialize(QueryId queryId)
    {
        nlohmann::json json;
        json["id"] = queryId.id;

        string result = json.dump();
        return result;
    }
    static shared_ptr<QueryId> Deserialize(string queryId)
    {
        nlohmann::json json = nlohmann::json::parse(queryId);
        return make_shared<QueryId>(json["id"]);
    }

};


#endif //OLVP_QUERYID_HPP
