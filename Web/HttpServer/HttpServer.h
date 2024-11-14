//
// Created by zxk on 7/26/24.
//

#ifndef OLVP_HTTPSERVER_H
#define OLVP_HTTPSERVER_H

#include "mongoose.h"
#include <memory>

class HttpServer
{

    static std::shared_ptr<struct mg_mgr> mgr;
public:
    HttpServer(){

    }
    static void start()
    {

        thread process([] {
          HttpServer::startHttpServer();
        });
        process.detach();

    }

    static void updateFileIp(string filePath,string serverIp)
    {
        ifstream input_file(filePath);
        ofstream output_file(filePath+"_temp");
        if (!input_file.is_open()) {
            spdlog::error("HttpServer could not open the file - '"+ filePath + "'");
            return;
        }


        string line;
        while(getline(input_file,line)){


            if(line.find("var") != string::npos && line.find("serverIp") != string::npos && line.find("'") != string::npos)
            {
                line = "var serverIp = '"+serverIp+"'";
            }

            output_file<<line;
            output_file<<"\n";
        }
        input_file.close();
        output_file.close();
        string rename = "mv "+filePath+"_temp "+filePath;
        system(rename.c_str());

    }

    static void resetAddressForAllFiles()
    {
        WebConfig config;
        string serverIP = config.getWebServerIp();
        string serverPort = config.getWebServerPort();

        string address = serverIP+":"+serverPort;

        if(config.getHttpServerAddress() != "")
        {
            address = config.getHttpServerAddress();
        }



        string indexFilePath = "./Front/index.html";
        string controlFilePath = "./Front/control.html";
        string resultFilePath = "./Front/result.html";

        updateFileIp(indexFilePath,address);
        updateFileIp(controlFilePath,address);
        updateFileIp(resultFilePath,address);
    }

    static void ev_handler(struct mg_connection *c, int ev, void *ev_data) {
        if (ev == MG_EV_HTTP_MSG) {
            struct mg_http_message *hm = (struct mg_http_message *) ev_data;
            struct mg_http_serve_opts opts = { .root_dir = "./Front/" };
            mg_http_serve_dir(c, hm, &opts);
        }
    }

    static void startHttpServer(void) {
        // Declare event manager
        HttpServer::mgr  = std::make_shared<struct mg_mgr>();
        mg_mgr_init(&(*mgr));  // Initialise event manager
        mg_http_listen(&(*mgr), "http://0.0.0.0:9082", ev_handler, NULL);  // Setup listener
        resetAddressForAllFiles();
        spdlog::info("HttpServer start!");
        for (;;) {          // Run an infinite event loop
            mg_mgr_poll(&(*mgr), 1000);
        }
    }

    static std::shared_ptr<struct mg_mgr> getHttpServer(void) {
        return HttpServer::mgr;
    }

    static void closeHttpServer()
    {
        mg_mgr_free(&(*HttpServer::mgr));
    }

};

std::shared_ptr<struct mg_mgr> HttpServer::mgr = NULL;

#endif //OLVP_HTTPSERVER_H
