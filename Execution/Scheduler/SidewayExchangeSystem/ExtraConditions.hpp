//
// Created by zxk on 6/8/26.
//

#ifndef OLVP_EXTRACONDITIONS_HPP
#define OLVP_EXTRACONDITIONS_HPP

class ExtraConditions {
    string extraName;
    vector<string> parameters;

public:
    ExtraConditions( string extraName,vector<string> parameters = {}) {
        this->extraName = extraName;
        this->parameters = parameters;
    }
    ExtraConditions() {
        this->extraName = "";
        this->parameters = {};
    }
    string getExtraConditionName(){return this->extraName;}
    vector<string> getExtraParameters(){return this->parameters;}

    static string Serialize(ExtraConditions extra_conditions)
    {
        nlohmann::json json;

        json["extraName"] = extra_conditions.extraName;
        json["parameters"] = extra_conditions.parameters;


        string result = json.dump();

        return result;
    }

    static ExtraConditions Deserialize(string migratedBufferAddress)
    {
        nlohmann::json json = nlohmann::json::parse(migratedBufferAddress);
        return ExtraConditions(json["extraName"],json["parameters"]);
    }

};

#endif //OLVP_EXTRACONDITIONS_HPP
