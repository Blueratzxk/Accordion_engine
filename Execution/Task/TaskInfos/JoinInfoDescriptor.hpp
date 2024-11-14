//
// Created by zxk on 7/29/24.
//

#ifndef OLVP_JOININFODESCRIPTOR_HPP
#define OLVP_JOININFODESCRIPTOR_HPP


class JoinInfoDescriptor
{
    int joinNums = 0;
    int buildNums = 0;
    double buildTime = 0;
    double buildComputingTime = 0;
public:

    JoinInfoDescriptor(){}

    JoinInfoDescriptor( int joinNums,int buildNums,double buildTime,double buildComputingTime)
    {
        this->joinNums = joinNums;
        this->buildNums = buildNums;
        this->buildTime = buildTime;
        this->buildComputingTime = buildComputingTime;
    }

    int getJoinNums(){return this->joinNums;}
    int getBuildNums(){return this->buildNums;}
    double getBuildTime(){return this->buildTime;}
    double getBuildComputingTime(){return this->buildComputingTime;}


    static string Serialize(JoinInfoDescriptor joinInfoDescriptor)
    {
        nlohmann::json json;

        json["joinNums"] = joinInfoDescriptor.joinNums;
        json["buildNums"] = joinInfoDescriptor.buildNums;
        json["buildTime"] = joinInfoDescriptor.buildTime;
        json["buildComputingTime"] = joinInfoDescriptor.buildComputingTime;


        string result = json.dump();
        return result;
    }

    static shared_ptr<JoinInfoDescriptor> Deserialize(string joinInfoDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(joinInfoDescriptor);

        auto result = make_shared<JoinInfoDescriptor>(json["joinNums"],
                                                        json["buildNums"],
                                                        json["buildTime"],
                                                        json["buildComputingTime"]);

        return  result;
    }




};


#endif //OLVP_JOININFODESCRIPTOR_HPP
