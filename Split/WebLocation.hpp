//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_WEBLOCATION_HPP
#define OLVP_WEBLOCATION_HPP

#include <string>
#include "nlohmann/json.hpp"
using namespace std;
class Location
{
    string ip;
    string port;
    string bufferId;
public:
    Location(string ip,string port,string bufferId){
        this->ip = ip;
        this->port = port;
        this->bufferId = bufferId;
    }
    string getIp()
    {
        return this->ip;
    }
    string getPort()
    {
        return this->port;
    }
    string getBufferId()
    {
        return this->bufferId;
    }
    string getLocation()
    {
        return ip+":"+port;
    }

    bool operator<(const Location &p) const //注意这里的两个const
    {
        return (ip < p.ip) || (port < p.port) || (bufferId < p.bufferId);
    }

    bool operator==(const Location &p) const //注意这里的两个const
    {
        return (ip == p.ip) && (port == p.port) && (bufferId == p.bufferId);
    }

    static string Serialize(Location location)
    {
        nlohmann::json json;
        json["ip"] = location.ip;
        json["port"] = location.port;
        json["bufferId"] = location.bufferId;

        string result = json.dump();
        return result;
    }
    static shared_ptr<Location> Deserialize(string location)
    {
        nlohmann::json json = nlohmann::json::parse(location);
        return make_shared<Location>(json["ip"],json["port"],json["bufferId"]);
    }







};

#endif //OLVP_WEBLOCATION_HPP
