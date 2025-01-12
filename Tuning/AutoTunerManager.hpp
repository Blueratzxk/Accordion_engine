//
// Created by zxk on 12/24/24.
//

#ifndef OLVP_AUTOTUNERMANAGER_HPP
#define OLVP_AUTOTUNERMANAGER_HPP


#include "ParallelismAutoTuner.hpp"

class AutoTunerManager : public enable_shared_from_this<AutoTunerManager>
{
    map<string,shared_ptr<ParallelismAutoTuner>> autoTuners;

    map<string,list<shared_ptr<AutoTuneInfo>>> autoTunersLog;

    mutex lock;

public:
    AutoTunerManager() = default;


    bool addAutoTuner(string queryId,shared_ptr<ParallelismAutoTuner> autoTuner)
    {
        lock.lock();
        if(!this->autoTuners.contains(queryId)) {
            this->autoTuners[queryId] = autoTuner;
            lock.unlock();
            return true;
        }
        else {
            lock.unlock();
            return false;
        }
    }

    void haltTuner(string queryId)
    {
        lock.lock();
        this->autoTuners[queryId]->haltTuner();
        lock.unlock();
    }

    void dumpAutoTuneLogs(string queryId)
    {
        auto logs = this->autoTuners[queryId]->getAutoTuneInfos();
        if(!this->autoTunersLog.contains(queryId)) {
            this->autoTunersLog[queryId] = {};
            for(auto log : logs)
                this->autoTunersLog[queryId].push_back(log);
        }
        else
        {
            for(auto log : logs)
                this->autoTunersLog[queryId].push_back(log);
        }
    }

    void removeAutoTuner(string queryId)
    {
        lock.lock();
        dumpAutoTuneLogs(queryId);
        this->autoTuners.erase(queryId);
        lock.unlock();
    }

    list<shared_ptr<AutoTuneInfo>> getAutoTuneInfos(string queryId)
    {
        list<shared_ptr<AutoTuneInfo>> logs;
        lock.lock();
        logs = this->autoTunersLog[queryId];
        lock.unlock();
        return logs;
    }

    string autoTuneByTimeConstraint(string queryId,shared_ptr<PPM> ppm, string timeConstraintBySeconds) {

        lock.lock();
        if(this->autoTuners.contains(queryId)){
            spdlog::info("Auto tuner already running!");
            if(this->autoTuners[queryId]->isMonitorMode() && atoi(timeConstraintBySeconds.c_str()) > 0) {
                dumpAutoTuneLogs(queryId);
                this->autoTuners[queryId]->haltTuner();
            }
            lock.unlock();
            return "Auto tuner already running!";
        }


        ExecutionConfig config;
        string autoTuneByPlan = config.getAutoTuneByPlan();

        if (autoTuneByPlan == "true" && atoi(timeConstraintBySeconds.c_str()) > 0) {
            thread th(autoTuneOnce, shared_from_this(), queryId, ppm, timeConstraintBySeconds, true);
            th.detach();
            lock.unlock();
            return "DOP monitor activated!";
        }
        else {
            //thread th(autoTuneOnce, shared_from_this(), queryId, ppm, timeConstraintBySeconds, false);
            //th.detach();
            lock.unlock();
            return autoTuneOnce(shared_from_this(),queryId,ppm,timeConstraintBySeconds, false);
        }

        lock.unlock();
        return "yes!";
    }



    static string autoTuneOnce(shared_ptr<AutoTunerManager> autoTunerManager, string queryId,shared_ptr<PPM> ppm,string timeConstraint,bool forceTuneOnce = false)
    {
        string info = "ok!";
        std::string name = "autoTuner";
        pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());


        shared_ptr<ParallelismAutoTuner> autoTuner = make_shared<ParallelismAutoTuner>(ppm);
        autoTunerManager->addAutoTuner(queryId,autoTuner);

        spdlog::info("Start tuning!");

        ExecutionConfig config;
        string autoTuneByPlan = config.getAutoTuneByPlan();
        if(autoTuneByPlan == "true" && !forceTuneOnce)
            autoTuner->dopMonitor(queryId);
        else
            info = autoTuner->tune(queryId,timeConstraint);

        autoTunerManager->removeAutoTuner(queryId);
        spdlog::info("Tuning finished!");

        return info;
    }


};





#endif //OLVP_AUTOTUNERMANAGER_HPP
