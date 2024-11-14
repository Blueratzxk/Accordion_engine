//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_SYSTEM_HPP
#define OLVP_SYSTEM_HPP


#include "TaskServer.h"
#include "QueryServer.hpp"
#include "../Shell/Shell.hpp"
#include "ClusterServer.h"
#include "../Web/Restful/Server.hpp"
#include "../Web/ArrowRPC/ArrowRPCServer.hpp"
#include "../Web/HttpServer/HttpServer.h"

#include "../Utils/WebCommon.hpp"



#include <signal.h>

class System
{
    static shared_ptr<StatsEndpoint> stat;
    static shared_ptr<arrow::flight::FlightServerBase> serv;
    static bool shutdown;
    static shared_ptr<Shell> shell;
public:


    static void start()
    {
        signal(SIGINT, sigint_handler);
        System::stat = StartHttpServer();
        auto status = StartArrowRPCServer(&serv);
        HttpServer::start();
        ClusterServer::start();
        spdlog::info("Initializing...");
        while(!(checkStaticStructures() && checkServers()))sleep(100);
        spdlog::info("Initializing finished!");


        ClusterServer::startHeartbeat();
        System::stat->setShell(getShell());
        System::shell->start();
    }

    static void sigint_handler(int sig){
        if(sig == SIGINT){
      //      if(!System::shutdown) {
                System::shutdown = true;
                System::stat->shutdown();

                cout << endl;
                spdlog::info("RestfulServer closed!");
                auto status = System::serv->Shutdown();
                spdlog::info("RpcServer closed!");
                HttpServer::closeHttpServer();
                spdlog::info("HttpServer closed!");
                spdlog::info("Bye~");
                exit(0);
     //       }

        }
    }


    static bool checkStaticStructures()
    {
        if(TaskServer::getTaskServer() == NULL)
            return false;
        if(TaskServer::taskResourceManager == NULL)
            return false;
        if(QueryServer::queryServer == NULL)
            return false;
        if(ClusterServer::getNodesManager() == NULL)
            return false;

        return true;
    }

    static shared_ptr<Shell> getShell()
    {
        if(System::shell == NULL)
            System::shell = make_shared<Shell>();
        return System::shell;
    }

    static bool checkServers()
    {
        WebConfig webConfig;
        string restfulPort = webConfig.getWebServerPort();
        string arrowRPCPort = webConfig.getRPCServerPort();
        if(WebCommon::isPortUsed(atoi(restfulPort.c_str())) == false)
            return false;
        if(WebCommon::isPortUsed(atoi(arrowRPCPort.c_str())) == false)
            return false;

        return true;
    }


};

shared_ptr<StatsEndpoint> System::stat = NULL;
shared_ptr<arrow::flight::FlightServerBase> System::serv = NULL;
bool System::shutdown = false;
shared_ptr<Shell> System::shell = NULL;

#endif //OLVP_SYSTEM_HPP
