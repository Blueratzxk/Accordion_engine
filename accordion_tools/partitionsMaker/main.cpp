#include <sys/stat.h>
#include <iostream>

#include <fstream>

#include <arrow/csv/api.h>
#include <arrow/csv/writer.h>
#include <arrow/csv/type_fwd.h>
#include <arrow/ipc/writer.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <sstream>
#include "spdlog/spdlog.h"
#include "nlohmann/json.hpp"

using namespace std;



class TableInfosAnalyzer
{

    shared_ptr<std::ifstream> jfile;


    map<string,shared_ptr<vector<pair<string,string>>>> table2Splits;

public:

    TableInfosAnalyzer(string filename){


        jfile = make_shared<ifstream>(filename);

        if(!jfile->is_open())
        {
            spdlog::error("Cannot open file DataFileDicts!");
            exit(0);
        }

    }

    TableInfosAnalyzer(){

    }


     void Stringsplit(std::string str, const char split,std::vector<std::string>& res)
    {
        std::istringstream iss(str);	// 输入流
        std::string token;			// 接收缓冲区
        while (getline(iss, token, split))	// 以split为分隔符
        {
            res.push_back(token);
        }
    }

    void analyze()
    {
        nlohmann::json j;
        *jfile >> j;

        nlohmann::json  js = nlohmann::json::array();
        js = j["tpch_test"]["tables"];

        for(auto jj : js)
        {
            string tableName = jj["tableName"];

            nlohmann::json splits = nlohmann::json::array();
            splits = jj["distributedFilePaths"];

            map<string,string> splitInfos;

            if(table2Splits.find(tableName) == table2Splits.end())
            {
                this->table2Splits[tableName] = make_shared<vector<pair<string,string>>>();
            }

            for(auto split : splits)
            {
                this->table2Splits[tableName]->push_back({split["netAddr"],split["fileAddr"]});
            }

        }
    }


    void customSplit(string str, char separator,vector<string> &strings) {
        int startIndex = 0, endIndex = 0;
        for (int i = 0; i <= str.size(); i++) {

            // If we reached the end of the word or the end of the input.
            if (str[i] == separator || i == str.size()) {
                endIndex = i;
                string temp;
                temp.append(str, startIndex, endIndex - startIndex);
                strings.push_back(temp);
                startIndex = endIndex + 1;
            }
        }
    }

    void sendAllPartitions(string prefix,string u,string p)
    {
        ofstream of("makeDFS.sh");
        of<<"#!/bin/bash"<<endl;
        for (auto table : this->table2Splits)
        {
            string originFileName = table.first;
            originFileName += ".tbl";


            for(int i = 0 ; i < table.second->size();i++)
            {
                string splitName;
                if(table.second->size() == 1)
                    splitName = originFileName;
                else
                    splitName = originFileName+"_"+to_string(i);
                string user = u;

                string destAddr = (*table.second)[i].first;
                vector<string> strings;
                customSplit(destAddr,':',strings);

                string destIP = strings[0];
                string destPath = (*table.second)[i].second;

                if(prefix != "")
                {
                    destPath = prefix+destPath;
                }

                string passwd = p;

                of << "bash scpFile.sh "+splitName+" "+user+" "+destIP+" "+destPath+" "+passwd << endl;
            }



        }

    }


    //table, node nums,partition nums per node
    std::map<string,pair<int,int>> tableMaps ={
            {"nation",{1,1}},
            {"supplier",{1,1}},
            {"region",{1,1}},
            {"part",{1,1}},
            {"partsupp",{4,1}},
            {"customer",{4,1}},
            {"orders",{4,1}},
            {"lineitem",{4,2}}

    };


    nlohmann::json genPartitionsForTable(nlohmann::json &table,vector<string> slaves)
    {
        string tableName = table["tableName"];

        int nodesNumForTable = tableMaps[tableName].first;
        int partitionNumForTable = tableMaps[tableName].second;

        nlohmann::json paths = nlohmann::json::array();

        int tableNumber = 0;

        if(nodesNumForTable == 1 && partitionNumForTable == 1)
        {
            nlohmann::json info;
            info["netAddr"] = slaves[0]+":9080";
            info["fileAddr"] = "data/"+tableName+".tbl";
            paths.push_back(info);
        }
        else {
            for (int i = 0; i < nodesNumForTable; i++) {
                for (int j = 0; j < partitionNumForTable; j++) {
                    nlohmann::json info;
                    info["netAddr"] = slaves[i] + ":9080";
                    info["fileAddr"] = "data/" + tableName + ".tbl_" + to_string(tableNumber);
                    tableNumber++;
                    paths.push_back(info);
                }

            }
        }

        table["distributedFilePaths"] = paths;

        return table;

    }





    void assignNodesforDataDictFile(string dataDictPath,string slavesPath,string tablePmapPath)
    {

        ifstream tablemap(tablePmapPath);
        if(tablemap.is_open())
        {
            string tmap;

            while(getline(tablemap,tmap))
            { vector<string> res;
                Stringsplit(tmap,',',res);

                if(tableMaps.count(res[0]) > 0)
                {
                    auto p = make_pair<int,int>(atoi(res[1].c_str()),atoi(res[2].c_str()));

                    cout << p.first <<"-"<<p.second << endl;
                    tableMaps[res[0]] = p;
                }

            }

        }
        else
        {
            cout << "No map file!"<<endl;
        }

        nlohmann::json datadict;
        ifstream jfile(dataDictPath);
        jfile >> datadict;


        ifstream slaves(slavesPath);
        vector<string> slaveIps;

        string read;
        while(getline(slaves,read))
        {
            slaveIps.push_back(read);
        }



        auto tableArrayNew = nlohmann::json::array();
        for(auto &item : datadict.items())
        {
            for(int i = 0 ; i <  (item.value())["tables"].size() ;i++) {
                genPartitionsForTable((item.value())["tables"][i],slaveIps);
            }
        }


        ofstream output("DataFileDicts.out");
        output << datadict.dump(1);

    }



    void makeDFSFile(string prefix,string u,string p)
    {
        ofstream of("makeDFS.sh");
        of<<"#!/bin/bash"<<endl;
        for (auto table : this->table2Splits)
        {
            string originFileName = table.first;
            originFileName += ".tbl";


            for(int i = 0 ; i < table.second->size();i++)
            {
                string splitName;
                if(table.second->size() == 1)
                    splitName = originFileName;
                else
                    splitName = originFileName+"_"+to_string(i);
                string user = u;

                string destAddr = (*table.second)[i].first;
                vector<string> strings;
                customSplit(destAddr,':',strings);

                string destIP = strings[0];
                string destPath = (*table.second)[i].second;

                if(prefix != "")
                {
                    destPath = prefix+destPath;
                }

                string passwd = p;

                of << "bash scpFile.sh "+splitName+" "+user+" "+destIP+" "+destPath+" "+passwd << endl;
            }



        }

    }




};


int main(int argc, char* argv[]) {

    string fileName = string(argv[1]);
    string user = string(argv[2]);
    string passwd = string(argv[3]);
    string preFix = string(argv[4]);

    TableInfosAnalyzer tableInfosAnalyzer;
    string datafiledictpath = string(argv[5]);
    string slavePath = string(argv[6]);
    string tablePartitions = string(argv[7]);

    tableInfosAnalyzer.assignNodesforDataDictFile(datafiledictpath,slavePath,tablePartitions);

    TableInfosAnalyzer tableInfosAnalyzer2("DataFileDicts.out");
    tableInfosAnalyzer2.analyze();
    tableInfosAnalyzer2.sendAllPartitions(preFix,user,passwd);
    return 0;
}
