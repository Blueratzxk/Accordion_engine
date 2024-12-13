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
    map<string,double> joinIdToBuildTime;
public:

    JoinInfoDescriptor(){}

    JoinInfoDescriptor( int joinNums,int buildNums,double buildTime,double buildComputingTime,map<string,double> joinIdToBuildTime)
    {
        this->joinNums = joinNums;
        this->buildNums = buildNums;
        this->buildTime = buildTime;
        this->buildComputingTime = buildComputingTime;
        this->joinIdToBuildTime = joinIdToBuildTime;
    }

    int getJoinNums(){return this->joinNums;}
    int getBuildNums(){return this->buildNums;}
    double getBuildTime(){return this->buildTime;}
    double getBuildComputingTime(){return this->buildComputingTime;}
    map<string,double> getJoinIdToBuildTime(){return this->joinIdToBuildTime;}

    static string Serialize(JoinInfoDescriptor joinInfoDescriptor)
    {
        nlohmann::json json;

        json["joinNums"] = joinInfoDescriptor.joinNums;
        json["buildNums"] = joinInfoDescriptor.buildNums;
        json["buildTime"] = joinInfoDescriptor.buildTime;
        json["buildComputingTime"] = joinInfoDescriptor.buildComputingTime;
        json["joinIdToBuildTime"] = joinInfoDescriptor.joinIdToBuildTime;

        string result = json.dump();
        return result;
    }

    static shared_ptr<JoinInfoDescriptor> Deserialize(string joinInfoDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(joinInfoDescriptor);

        auto result = make_shared<JoinInfoDescriptor>(json["joinNums"],
                                                        json["buildNums"],
                                                        json["buildTime"],
                                                        json["buildComputingTime"],
                                                        json["joinIdToBuildTime"]);

        return  result;
    }




};


#endif //OLVP_JOININFODESCRIPTOR_HPP
