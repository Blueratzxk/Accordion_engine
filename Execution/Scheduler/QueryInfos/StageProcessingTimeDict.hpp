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

public:
    SPTD_DOP_Time(int DOP, long time)
    {
        this->DOP = DOP;
        this->time = time;
    }

    int getDOP(){return DOP;}
    long getTime(){return time;}
    void setTime(long newTime){this->time = newTime;}

    static nlohmann::json Serialize(SPTD_DOP_Time sptdDopTime)
    {
        nlohmann::json json;
        json["DOP"] = sptdDopTime.DOP;
        json["time"] = sptdDopTime.time;
        return json;
    }
    static SPTD_DOP_Time Deserialize(nlohmann::json json){
        return {json["DOP"],json["time"]};
    }

};

class SPTD_Infolist
{
    map<int,vector<SPTD_DOP_Time>> SPTD_DOP_Times;



public:
    SPTD_Infolist(map<int,vector<SPTD_DOP_Time>> SPTD_DOP_Times){

        this->SPTD_DOP_Times = SPTD_DOP_Times;
    }

    void addInfo(int stageId,SPTD_DOP_Time sptdDopTime)
    {
        bool findItem = false;
        if(SPTD_DOP_Times.contains(stageId)) {
            for(int i = 0 ; i < SPTD_DOP_Times[stageId].size() ; i++){
                if(SPTD_DOP_Times[stageId][i].getDOP() == sptdDopTime.getDOP()) {
                    findItem = true;
                    SPTD_DOP_Times[stageId][i].setTime(sptdDopTime.getTime());
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


    void addNewInfo(string queryName,int stageId,int dop,long processingTime)
    {
        SPTD_DOP_Time sptdDopTime(dop,processingTime);

        if(!dicts.contains(queryName)) {
            SPTD_Infolist sptdInfolist;
            dicts[queryName] = sptdInfolist;
            dicts[queryName].addInfo(stageId,sptdDopTime);
        }
        else
            dicts[queryName].addInfo(stageId,sptdDopTime);
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
