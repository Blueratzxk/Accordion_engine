//
// Created by zxk on 10/23/25.
//

#ifndef OLVP_SIDEWAYMISSIONINFODESCRIPTOR_HPP
#define OLVP_SIDEWAYMISSIONINFODESCRIPTOR_HPP



class SidewayMissionInfoDescriptor
{
    map<string,int> sidewayUploadTuples;
    map<string,int> sidewayFulfillTuples;
public:

    SidewayMissionInfoDescriptor(){}

    SidewayMissionInfoDescriptor(map<string,int> sidewayUploadTuples, map<string,int> sidewayFulfillTuples)
    {
       this->sidewayFulfillTuples = sidewayFulfillTuples;
       this->sidewayUploadTuples = sidewayUploadTuples;
    }

    map<string,int> getSidewayUploadTuples(){return this->sidewayUploadTuples;}
    map<string,int> getSidewayFulfillTuples(){return this->sidewayFulfillTuples;}

    static string Serialize(SidewayMissionInfoDescriptor sidewayMissionInfoDescriptor)
    {
        nlohmann::json json;

        json["sidewayUploadTuples"] = sidewayMissionInfoDescriptor.sidewayUploadTuples;
        json["sidewayFulfillTuples"] = sidewayMissionInfoDescriptor.sidewayFulfillTuples;


        string result = json.dump();
        return result;
    }

    static shared_ptr<SidewayMissionInfoDescriptor> Deserialize(string sidewayMissionInfoDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(sidewayMissionInfoDescriptor);

        auto result = make_shared<SidewayMissionInfoDescriptor>(json["sidewayUploadTuples"], json["sidewayFulfillTuples"]);

        return  result;
    }




};



#endif //OLVP_SIDEWAYMISSIONINFODESCRIPTOR_HPP
