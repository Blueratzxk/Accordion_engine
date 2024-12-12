//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_WEBCONFIG_HPP
#define OLVP_WEBCONFIG_HPP


#include "nlohmann/json.hpp"
#include <string>
#include <iostream>
#include <fstream>
//#include "../Utils/Random.hpp"

using namespace std;

class WebConfig
{
    bool hasRead = false;
    string Restful_Web_Server_IP;
    string Restful_Web_Server_Port;
    string Arrow_RPC_Server_IP;
    string Arrow_RPC_Server_Port;

    string Coordinator_Restful_Web_Server_IP;
    string Coordinator_Restful_Web_Server_Port;
    string Coordinator_Arrow_RPC_Server_IP;
    string Coordinator_Arrow_RPC_Server_Port;

    string NIC_Name;
    string NIC_speed = "";
    string HttpServerAddress = "";
public:
    WebConfig(){}

    bool readConfigFile()
    {
        if(hasRead == true)
            return true;
        string strFileData = "httpconfig.config";
        std::ifstream in(strFileData, std::ios::in | std::ios::binary);
        if (!in.is_open())
        {
            cout << "Cannot open the web config file!"<<endl;
            exit(0);
            return false;
        }
        nlohmann::json jsonTree = nlohmann::json::parse(in);
        nlohmann::json json = jsonTree["local"];
        this->Restful_Web_Server_IP = json["Restful_Web_Server_IP"];
        this->Restful_Web_Server_Port = json["Restful_Web_Server_Port"];


        this->Arrow_RPC_Server_IP = json["Arrow_RPC_Server_IP"];
        this->Arrow_RPC_Server_Port = json["Arrow_RPC_Server_Port"];


        nlohmann::json jsonCoor = jsonTree["coordinator"];
        this->Coordinator_Restful_Web_Server_IP = jsonCoor["Restful_Web_Server_IP"];
        this->Coordinator_Restful_Web_Server_Port = jsonCoor["Restful_Web_Server_Port"];


        this->Coordinator_Arrow_RPC_Server_IP = jsonCoor["Arrow_RPC_Server_IP"];
        this->Coordinator_Arrow_RPC_Server_Port = jsonCoor["Arrow_RPC_Server_Port"];

        this->NIC_Name = jsonTree["nic"];

        auto result = jsonTree.find("HttpServerAddress");
        if(result != jsonTree.end())
        {
            this->HttpServerAddress = jsonTree["HttpServerAddress"];
        }

        auto nicspeed = jsonTree.find("nic_speed");
        if(nicspeed != jsonTree.end())
        {
            this->NIC_speed = jsonTree["nic_speed"];
        }


        in.close();
        this->hasRead = true;
        return true;
    }

    bool thisNodeIsCoordinator()
    {
        readConfigFile();
        if(this->Restful_Web_Server_IP == this->Coordinator_Restful_Web_Server_IP && this->Arrow_RPC_Server_IP == this->Coordinator_Arrow_RPC_Server_IP
        && this->Restful_Web_Server_Port == this->Coordinator_Restful_Web_Server_Port && this->Arrow_RPC_Server_Port == this->Coordinator_Arrow_RPC_Server_Port)
            return true;
        else
            return false;
    }

    string getWebServerIp()
    {
        readConfigFile();
        return this->Restful_Web_Server_IP;
    }
    string getWebServerPort()
    {
        readConfigFile();
        return this->Restful_Web_Server_Port;
    }
    string getRPCServerIp()
    {
        readConfigFile();
        return this->Arrow_RPC_Server_IP;
    }
    string getRPCServerPort()
    {
        readConfigFile();
        return this->Arrow_RPC_Server_Port;
    }


    string getNIC_Name()
    {
        readConfigFile();
        return this->NIC_Name;
    }
    string getNIC_Speed()
    {
        readConfigFile();
        return this->NIC_speed;
    }
    string getHttpServerAddress()
    {
        readConfigFile();
        return this->HttpServerAddress;
    }
    string getCoordinatorWebServerIp()
    {
        readConfigFile();
        return this->Coordinator_Restful_Web_Server_IP;
    }
    string getCoordinatorWebServerPort()
    {
        readConfigFile();
        return this->Coordinator_Restful_Web_Server_Port;
    }
    string getCoordinatorRPCServerIp()
    {
        readConfigFile();
        return this->Coordinator_Arrow_RPC_Server_IP;
    }
    string getCoordinatorRPCServerPort()
    {
        readConfigFile();
        return this->Coordinator_Arrow_RPC_Server_Port;
    }
};


#endif //OLVP_WEBCONFIG_HPP
