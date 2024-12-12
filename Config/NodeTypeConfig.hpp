//
// Created by zxk on 12/22/23.
//

#ifndef OLVP_NODETYPECONFIG_HPP
#define OLVP_NODETYPECONFIG_HPP


#include "nlohmann/json.hpp"
#include <string>
#include <iostream>
#include <fstream>
#include <set>

using namespace std;

class NodeTypeConfig
{
    bool hasRead = false;

    bool hasKnown = false;
    bool isStorageNode = false;

    set<string> netAddrs;

public:
    NodeTypeConfig(){

    }

    bool readConfigFile()
    {
        if(hasRead == true)
            return true;


        string strFileData = "DataFileDicts";
        std::ifstream in(strFileData, std::ios::in | std::ios::binary);
        if (!in.is_open())
        {
            cout << "Cannot open the node type config file!"<<endl;
            exit(0);
            return false;
        }
        nlohmann::json jsonTree = nlohmann::json::parse(in);


        nlohmann::json tables = nlohmann::json::array();
        tables = jsonTree["tpch_test"]["tables"];


        for(auto table : tables) {
            for (auto net: table["distributedFilePaths"])
            {
                netAddrs.insert(net["netAddr"]);
            }
        }


        in.close();
        this->hasRead = true;
        return true;
    }

    bool isStorage(string addr)
    {
        if(!hasRead)
            readConfigFile();
        if(hasKnown)
            return this->isStorageNode;

        if(this->netAddrs.contains(addr)) {
            this->hasKnown = true;
            this->isStorageNode = true;
            return true;
        }
        else {
            this->hasKnown = true;
            this->isStorageNode = false;
            return false;
        }

    }



};


#endif //OLVP_NODETYPECONFIG_HPP
