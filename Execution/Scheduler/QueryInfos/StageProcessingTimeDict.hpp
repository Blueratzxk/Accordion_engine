//
// Created by zxk on 12/13/24.
//

#ifndef OLVP_STAGEPROCESSINGTIMEDICT_HPP
#define OLVP_STAGEPROCESSINGTIMEDICT_HPP

#include <vector>
#include <map>
#include <string>
#include <fstream>
#include "nlohmann/json.hpp"

using namespace std;


class SPTD_DOP_Time
{
    int DOP;
    long time;
    long procTime = 0;
    long buildTime = 0;

public:

    SPTD_DOP_Time(int DOP, long time,long procTime,long buildTime)
    {
        this->DOP = DOP;
        this->time = time;
        this->procTime = procTime;
        this->buildTime = buildTime;
    }

    int getDOP(){return DOP;}
    long getTime(){return time;}
    long getProcTime(){return procTime;}
    long getBuildTime(){return buildTime;}
    void setTime(long newTime,long newProc,long newBuild){this->time = newTime;this->procTime = newProc;this->buildTime = newBuild;}

    static nlohmann::json Serialize(SPTD_DOP_Time sptdDopTime)
    {
        nlohmann::json json;
        json["DOP"] = sptdDopTime.DOP;
        json["time"] = sptdDopTime.time;
        json["procTime"] = sptdDopTime.procTime;
        json["buildTime"] = sptdDopTime.buildTime;
        return json;
    }
    static SPTD_DOP_Time Deserialize(nlohmann::json json){
        return {json["DOP"],json["time"],json["procTime"],json["buildTime"]};
    }

};

class SPTD_Infolist
{
    map<int,vector<SPTD_DOP_Time>> SPTD_DOP_Times;



public:
    SPTD_Infolist(map<int,vector<SPTD_DOP_Time>> SPTD_DOP_Times){

        this->SPTD_DOP_Times = SPTD_DOP_Times;
    }

    long getTime(int stageId,int DOP)
    {
        if(this->SPTD_DOP_Times.contains(stageId))
        {
            auto DOP_Times = SPTD_DOP_Times[stageId];
            for(auto dop_time : DOP_Times)
            {
                if(dop_time.getDOP() == DOP)
                    return dop_time.getTime();
            }
        }

        return -1;
    }

    long getProcTime(int stageId,int DOP)
    {
        if(this->SPTD_DOP_Times.contains(stageId))
        {
            auto DOP_Times = SPTD_DOP_Times[stageId];
            for(auto dop_time : DOP_Times)
            {
                if(dop_time.getDOP() == DOP)
                    return dop_time.getProcTime();
            }
        }

        return -1;
    }

    long getBuildTime(int stageId,int DOP)
    {
        if(this->SPTD_DOP_Times.contains(stageId))
        {
            auto DOP_Times = SPTD_DOP_Times[stageId];
            for(auto dop_time : DOP_Times)
            {
                return dop_time.getBuildTime();
            }
        }

        return -1;
    }

    void addInfo(int stageId,SPTD_DOP_Time sptdDopTime)
    {
        bool findItem = false;
        if(SPTD_DOP_Times.contains(stageId)) {
            for(int i = 0 ; i < SPTD_DOP_Times[stageId].size() ; i++){
                if(SPTD_DOP_Times[stageId][i].getDOP() == sptdDopTime.getDOP()) {
                    findItem = true;
                    SPTD_DOP_Times[stageId][i].setTime(sptdDopTime.getTime(),sptdDopTime.getProcTime(),sptdDopTime.getBuildTime());
                }

            }
            if(!findItem)
                SPTD_DOP_Times[stageId].push_back(sptdDopTime);

        }
        else
        {
            SPTD_DOP_Times[stageId] = {};
            SPTD_DOP_Times[stageId].push_back(sptdDopTime);
        }
    }

    static nlohmann::json Serialize(SPTD_Infolist sptdInfolist)
    {

        nlohmann::json json;
        map<int,vector<nlohmann::json>> SPTD_DOP_Times_json;


        for(auto stage : sptdInfolist.SPTD_DOP_Times) {

            nlohmann::json jsonArray = nlohmann::json::array();

            for(auto item : stage.second) {
                jsonArray.push_back(SPTD_DOP_Time::Serialize(item));
            }
            SPTD_DOP_Times_json[stage.first] = jsonArray;
        }

        json["list"] = SPTD_DOP_Times_json;

        return json;
    }
    static SPTD_Infolist Deserialize(nlohmann::json json){

        map<int,vector<nlohmann::json>> SPTD_DOP_Times_json;
        SPTD_DOP_Times_json = json["list"];

        map<int,vector<SPTD_DOP_Time>> SPTD_DOP_Times;

        for(auto stage : SPTD_DOP_Times_json) {
            vector<SPTD_DOP_Time> array;
            for (auto item: stage.second)
                array.push_back(SPTD_DOP_Time::Deserialize(item));
            SPTD_DOP_Times[stage.first] = array;
        }

        return SPTD_Infolist(SPTD_DOP_Times);
    }

    SPTD_Infolist() {

    }
};

class StageProcessingTimeDict
{
    map<string,SPTD_Infolist> dicts;
    string queryMetaPath = "meta.json";
public:
    StageProcessingTimeDict(map<string,SPTD_Infolist> dicts){
        this->dicts = dicts;
    }
    StageProcessingTimeDict (){
        initMetaFile();
    }


    void addNewInfo(string queryName,int stageId,int dop,long processingTime,long procTime,long buildTime)
    {
        SPTD_DOP_Time sptdDopTime(dop,processingTime,procTime,buildTime);

        if(!dicts.contains(queryName)) {
            SPTD_Infolist sptdInfolist;
            dicts[queryName] = sptdInfolist;
            dicts[queryName].addInfo(stageId,sptdDopTime);
        }
        else
            dicts[queryName].addInfo(stageId,sptdDopTime);
    }

    long getProcessingTime(string queryName,int stageId,int dop)
    {
        if(!dicts.contains(queryName)) {
            return  -1;
        }
        else
            return dicts[queryName].getTime(stageId,dop);
    }
    long getProcTime(string queryName,int stageId,int dop)
    {
        if(!dicts.contains(queryName)) {
            return  -1;
        }
        else
            return dicts[queryName].getProcTime(stageId,dop);
    }

    long getBuildTime(string queryName,int stageId,int dop)
    {
        if(!dicts.contains(queryName)) {
            return  -1;
        }
        else
            return dicts[queryName].getBuildTime(stageId,dop);
    }

    void initMetaFile()
    {
        fstream fs;
        fs.open(queryMetaPath, ios::in);
        if (!fs)
        {
            ofstream fout(queryMetaPath);
            if (fout)
                fout.close();
        }
        else if (fs.peek() == std::ifstream::traits_type::eof())
            ;
        else {

            string j;
            ifstream jfile(queryMetaPath);
            jfile >> j;

            auto re = this->Deserialize(j);
            this->dicts = re.dicts;

            fs.close();
        }
    }

    void updateMetaFile(string data)
    {
        ofstream of(this->queryMetaPath);
        if (of.is_open())
            of << data;
        of.close();
    }

    void save()
    {
        string re = this->Serialize(*this);
        this->updateMetaFile(re);
    }

    string getJson(){
        string re = this->Serialize(*this);
        return re;
    }


    string Serialize(StageProcessingTimeDict stageProcessingTimeDict)
    {
        nlohmann::json json;
        for(auto stage : stageProcessingTimeDict.dicts) {

            nlohmann::json sptd_Infolist = SPTD_Infolist::Serialize(stage.second);
            json[stage.first] = sptd_Infolist;
        }
        return json.dump();
    }

    StageProcessingTimeDict Deserialize(string json)
    {
        auto jsonObj = nlohmann::json::parse(json);

        map<string,SPTD_Infolist> dicts;
        for(auto stage : jsonObj.items()) {
            auto sptd_Infolist = SPTD_Infolist::Deserialize(stage.value());
            dicts[stage.key()] = sptd_Infolist;
        }

        return StageProcessingTimeDict(dicts);
    }


};




#endif //OLVP_STAGEPROCESSINGTIMEDICT_HPP
