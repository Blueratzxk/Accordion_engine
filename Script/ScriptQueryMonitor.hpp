//
// Created by zxk on 11/18/23.
//

#ifndef OLVP_SCRIPTQUERYMONITOR_HPP
#define OLVP_SCRIPTQUERYMONITOR_HPP

#include "../Query/QueryManager.hpp"
#include <memory>
#include <string>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
class ScriptQueryMonitor
{
    shared_ptr<QueryManager> queryManager;


    mutex lock;
    map<string,shared_ptr<ofstream>> queryLogs;

    atomic<bool> monitorOn = false;

    string dirName = "monitorData";

    int IsFileExist(const char* path)
    {
        return !access(path, F_OK);
    }

public:
    ScriptQueryMonitor(){

        if(!IsFileExist(dirName.c_str()))
            mkdir(dirName.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);


    }

    void setQueryManager(shared_ptr<QueryManager> queryManager)
    {
        this->queryManager = queryManager;
    }

    void addQuery(string queryId)
    {
        lock.lock();
        if(queryLogs.count(queryId) == 0) {
            queryLogs[queryId] = make_shared<ofstream>();
            queryLogs[queryId]->open(dirName+"/"+queryId+"_Data");
        }

        if(!this->monitorOn)
        {
            thread(ScriptQueryMonitor::scriptMonitor,this).detach();
            this->monitorOn = true;
        }

        lock.unlock();
    }
    bool hasQueries()
    {
        bool yes = false;
        lock.lock();
        if(this->queryLogs.size() > 0)
            yes = true;
        lock.unlock();

        return yes;

    }
    void deleteQuery(string queryId)
    {
        lock.lock();
        queryLogs[queryId]->close();
        queryLogs.erase(queryId);
        lock.unlock();
    }

    void addInfo(string queryId,string info)
    {
        shared_ptr<ofstream> ff = NULL;
        lock.lock();
        if(this->queryLogs.count(queryId) > 0)
            ff = this->queryLogs[queryId];
        lock.unlock();

        if(ff != NULL)
            (*ff) << info << endl;
    }

    void collectInfo()
    {

        lock.lock();
        map<string,shared_ptr<ofstream>> qls = this->queryLogs;
        lock.unlock();
        for(auto query : qls)
        {

            if(!this->queryManager->isQueryStart(query.first))
                continue;
            string re = this->queryManager->getQueryThroughputsInfo(query.first);

            if(re == "NULL")
                continue;

            nlohmann::json j = nlohmann::json::parse(re);


            j["Type"] = "TP";
            j["TimeStamp"] = TimeCommon::getCurrentTimeStamp();
            (*query.second)<< j.dump() << endl;
        }
    }

    static void scriptMonitor(ScriptQueryMonitor *monitor){

        while(true)
        {
            if(!monitor->hasQueries()) {
                monitor->monitorOn = false;
                return;
            }
            sleep_for(std::chrono::milliseconds(100));
            monitor->collectInfo();

        }

    }


};

#endif //OLVP_SCRIPTQUERYMONITOR_HPP
