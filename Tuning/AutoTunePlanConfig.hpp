//
// Created by zxk on 12/23/24.
//

#ifndef OLVP_AUTOTUNEPLANCONFIG_HPP
#define OLVP_AUTOTUNEPLANCONFIG_HPP


#include <string>
#include <map>
#include <fstream>
#include <ostream>
#include <istream>
#include <vector>
#include "nlohmann/json.hpp"
using namespace std;

class AutoTunePlanConfig
{
    map<string,map<string,long>> queryNameToTimeConstraints;
    string planPath = "plan.json";
public:
    AutoTunePlanConfig()
    {
        initFile();
    }

    void planTemplate()
    {
        queryNameToTimeConstraints["Q1"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q2"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q3"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q4"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q5"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q6"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q7"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q8"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q9"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q10"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q11"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q12"] = {{"S1",-1},{"S2",-1}};
        queryNameToTimeConstraints["Q2_J"] = {{"S1",-1},{"S2",-1}};

        nlohmann::json json = queryNameToTimeConstraints;
        ofstream of(this->planPath);
        if (of.is_open())
            of << json.dump(1);
        of.close();

    }
    void initFile()
    {
        fstream fs;
        fs.open(planPath, ios::in);
        if (!fs)
        {
            ofstream fout(planPath);
            if (fout)
                fout.close();
            planTemplate();
        }
        else if (fs.peek() == std::ifstream::traits_type::eof())
            planTemplate();
        else {
            ifstream jfile(planPath);
            std::stringstream buffer;
            buffer << jfile.rdbuf();
            std::string j(buffer.str());

            auto res = nlohmann::json::parse(j);

            for(auto item : res.items())
                this->queryNameToTimeConstraints[item.key()] = item.value();

            //this->queryNameToTimeConstraints = res;
            fs.close();
        }
    }


    long getTimeConstraint(string queryName,string stageId)
    {
        if(!this->queryNameToTimeConstraints.contains(queryName))
            return -1;

        auto timeConstraints = this->queryNameToTimeConstraints[queryName];

        for(auto constraint : timeConstraints)
            if(constraint.first == ("S"+stageId))
                return constraint.second;
        return -1;

    }

};


#endif //OLVP_AUTOTUNEPLANCONFIG_HPP
