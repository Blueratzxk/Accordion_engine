//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_CONNECTORID_HPP
#define OLVP_CONNECTORID_HPP

#include <string>
#include "nlohmann/json.hpp"

using namespace std;
class ConnectorId
{
    string catalogName;


public:
    ConnectorId(){}
    ConnectorId(string catalogName){
        this->catalogName = catalogName;
    }

    string getCatalogName() const {
        return this->catalogName;
    }


    static string Serialize(ConnectorId connectorId)
    {
        nlohmann::json json;

        json["catalogName"] = connectorId.catalogName;

        string result = json.dump();
        return result;
    }
    static shared_ptr<ConnectorId> Deserialize(string connectorId)
    {
        nlohmann::json json = nlohmann::json::parse(connectorId);
        return make_shared<ConnectorId>(json["catalogName"]);
    }


    struct compare {
        static size_t hash( const ConnectorId& x ) {
            size_t h = 0;
            for( const char* s = (x.catalogName).c_str(); *s; ++s )
                h = (h*17)^*s;
            return h;
        }
        //! True if strings are equal
        static bool equal( const ConnectorId& x, const ConnectorId& y ) {
            return (x.catalogName) == (y.catalogName);
        }
    };

};



#endif //OLVP_CONNECTORID_HPP
