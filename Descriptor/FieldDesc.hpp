//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_FIELDDESC_HPP
#define OLVP_FIELDDESC_HPP

#include "nlohmann/json.hpp"
class FieldDesc
{

    string name;
    string type;
public:
    FieldDesc(string name,string type)
    {
        this->name = name;
        this->type = type;
    }

    string getFieldName()
    {
        return this->name;
    }
    string getFieldType()
    {
        return this->type;
    }

    bool equals(FieldDesc field)
    {
        if(this->type == field.type && this->name == field.name)
            return true;
        else
            return false;
    }
    bool isEmpty()
    {
        if(this->type == "" || this->name == "")
            return true;
        else
            return false;
    }
    static FieldDesc getEmptyDesc()
    {
        return FieldDesc("","");
    }


    static string Serialize(FieldDesc fieldDesc)
    {
        nlohmann::json desc;

        desc["name"] = fieldDesc.name;
        desc["type"] = fieldDesc.type;

        string result = desc.dump();

        return result;
    }


    static string SerializeObjects(vector<FieldDesc> fieldDesc)
    {
        nlohmann::json desc;


        for(int i = 0 ; i < fieldDesc.size() ; i++)
            desc.push_back(FieldDesc::Serialize(fieldDesc[i]));

        string result = desc.dump();

        return result;
    }

    static vector<FieldDesc> DeserializeObjects(string fieldDescs)
    {

        nlohmann::json desc = nlohmann::json::array();

        desc = nlohmann::json::parse(fieldDescs);

        vector<FieldDesc> fieldDescriptors;
        for(auto field : desc)
        {
            fieldDescriptors.push_back(FieldDesc::Deserialize(field));
        }

        return fieldDescriptors;
    }

    static FieldDesc Deserialize(string desc)
    {
        nlohmann::json field = nlohmann::json::parse(desc);
        return FieldDesc(field["name"],field["type"]);
    }

    string to_string()
    {
        string result;

        return result+"Field{"+"name:"+this->name+"|"+"type:"+this->type+"}"+"\n";
    }


};

#endif //OLVP_FIELDDESC_HPP
