//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_SPLITSERIAL_HPP
#define OLVP_SPLITSERIAL_HPP

#include "../RemoteSplit.hpp"
#include "../TpchSplit.hpp"
#include "../Split.hpp"

class SplitSerial
{
public:

    SplitSerial()
    {

    }


    static bool getConnectorSplitString(shared_ptr<ConnectorSplit> connectorSplit,string &result,string &type)
    {
        if(connectorSplit->getId() == "TpchSplit") {
            result = TpchSplit::Serialize(*static_pointer_cast<TpchSplit>(connectorSplit));
            type = "TpchSplit";
        }
        else if(connectorSplit->getId() == "RemoteSplit") {
            result = RemoteSplit::Serialize(*static_pointer_cast<RemoteSplit>(connectorSplit));
            type = "RemoteSplit";
        }
        else {
            spdlog::critical("Unsupported ConnectorSplit "+connectorSplit->getId()+"!");
            return false;
        }

        return true;
    }

    static shared_ptr<ConnectorSplit> getConnectorSplitObject(string connectorSplitString,string type)
    {
        shared_ptr<ConnectorSplit> connectorSplit;

        if(type == "TpchSplit") {
            connectorSplit = TpchSplit::Deserialize(connectorSplitString);
            type = "TpchSplit";
        }
        else if(type == "RemoteSplit") {
            connectorSplit = RemoteSplit::Deserialize(connectorSplitString);
            type = "RemoteSplit";
        }
        else {
            spdlog::critical("Unsupported ConnectorSplit "+connectorSplit->getId()+"!");
            return NULL;
        }

        return connectorSplit;
    }





    static string Serialize(Split split)
    {
        nlohmann::json json;

        json["connectorId"] = ConnectorId::Serialize(split.getConnectorId());
        string connectorSplitString;
        string type;
        if(!getConnectorSplitString(split.getConnectorSplit(),connectorSplitString,type))
            return NULL;
        json["connectorSplit"] =  connectorSplitString;
        json["connectorSplitType"] = type;

        string result = json.dump();

        return result;
    }

    static shared_ptr<Split> Deserialize(string split)
    {
        nlohmann::json json = nlohmann::json::parse(split);

        return make_shared<Split>(*ConnectorId::Deserialize(json["connectorId"]), getConnectorSplitObject(json["connectorSplit"], json["connectorSplitType"]));
    }

};


#endif //OLVP_SPLITSERIAL_HPP
