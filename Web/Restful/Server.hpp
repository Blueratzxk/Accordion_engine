//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_SERVER_HPP
#define OLVP_SERVER_HPP



#include <algorithm>

#include <pistache/http.h>
#include <pistache/http_header.h>
#include <pistache/http_headers.h>
#include <pistache/router.h>
#include <pistache/endpoint.h>

#include "Client.hpp"

#include "base64.hpp"
#include "../../Config/WebConfig.hpp"
#include "../TaskServerInferface.hpp"
#include "../../Utils/Random.hpp"
#include "../QueryInterface.hpp"
#include "../ClusterInterface.hpp"
#include "../../Shell/Shell.hpp"

using namespace std;
using namespace Pistache;






class StatsEndpoint {


    typedef std::mutex Lock;
    typedef std::lock_guard<Lock> Guard;
    Lock metricsLock;


    std::shared_ptr<Http::Endpoint> httpEndpoint;
    Rest::Router router;
    shared_ptr<Shell> shell;

public:
    StatsEndpoint(Address addr)
            : httpEndpoint(std::make_shared<Http::Endpoint>(addr))
    { }

    void init(size_t thr = 2) {

        auto opts = Http::Endpoint::options().maxRequestSize(1024*1024)
                .threads(thr)
                .flags(Tcp::Options::ReuseAddr | Tcp::Options::ReusePort);
        httpEndpoint->init(opts);
        setupRoutes();
    }

    void setShell(shared_ptr<Shell> shell)
    {
        this->shell = shell;
    }

    void start() {


        httpEndpoint->setHandler(router.handler());
        httpEndpoint->serve();
    }

    void shutdown() {
        httpEndpoint->shutdown();
    }


private:
    void setupRoutes() {
        using namespace Rest;

        Routes::Post(router, "/v1/record/:name/:value", Routes::bind(&StatsEndpoint::doRecordMetric, this));


        Routes::Post(router, "/v1/task/updateTask/:taskIdLength/:taskId/:updateRequest_Length/:updateRequest", Routes::bind(&StatsEndpoint::updateTask, this));
        Routes::Post(router, "/v1/task/getTaskInfo/:taskIdLength/:taskId", Routes::bind(&StatsEndpoint::getTaskInfo, this));
        Routes::Post(router, "/v1/task/closeTask/:taskIdLength/:taskId", Routes::bind(&StatsEndpoint::closeTask, this));


        Routes::Get(router, "/v1/task/getTaskInfoExtern/:taskId", Routes::bind(&StatsEndpoint::getTaskInfoExtern, this));
        Routes::Get(router, "/v1/task/getAllTaskInfoExtern", Routes::bind(&StatsEndpoint::getAllTaskInfoExtern, this));





        Routes::Get(router, "/v1/query/getAllRunningQueryInfoExtern", Routes::bind(&StatsEndpoint::getAllRunningQueryInfoExtern, this));
        Routes::Get(router, "/v1/query/getAllQueryInfoExtern", Routes::bind(&StatsEndpoint::getAllQueryInfoExtern, this));
        Routes::Get(router, "/v1/query/getQueryInfoExtern/:queryId", Routes::bind(&StatsEndpoint::getQueryInfoExtern, this));
        Routes::Get(router, "/v1/query/getQueryResultExtern/:queryId", Routes::bind(&StatsEndpoint::getQueryResultExtern, this));
        Routes::Get(router, "/v1/query/addStageConcurrentExtern/:queryId/:stageId", Routes::bind(&StatsEndpoint::addStageConcurrentExtern, this));
        Routes::Get(router, "/v1/query/decreaseStageParallelismExtern/:queryId/:stageId", Routes::bind(&StatsEndpoint::decreaseStageParallelismExtern, this));
        Routes::Get(router, "/v1/query/addStageTaskGroupConcurrentExtern/:queryId/:stageId/:taskNum", Routes::bind(&StatsEndpoint::addStageTaskGroupConcurrentExtern, this));
        Routes::Get(router, "/v1/query/addStageAllTaskPipelineConcurrentExtern/:queryId/:stageId/:pipelineId", Routes::bind(&StatsEndpoint::addStageAllTaskPipelineConcurrentExtern, this));
        Routes::Get(router, "/v1/query/subStageAllTaskPipelineConcurrentExtern/:queryId/:stageId/:pipelineId", Routes::bind(&StatsEndpoint::subStageAllTaskPipelineConcurrentExtern, this));


        Routes::Get(router, "/v1/query/giveMeAQueryExtern/:queryId", Routes::bind(&StatsEndpoint::giveMeAQueryExtern, this));
        Routes::Get(router, "/v1/query/listAllQueriesExtern", Routes::bind(&StatsEndpoint::listAllQueriesExtern, this));
        Routes::Get(router, "/v1/query/getQueryThroughputsExtern/:queryId", Routes::bind(&StatsEndpoint::getQueryThroughputsExtern, this));
        Routes::Get(router, "/v1/query/getQueryThroughputsInfoExtern/:queryId", Routes::bind(&StatsEndpoint::getQueryThroughputsInfoExtern, this));
        Routes::Get(router, "/v1/query/getQueryStagePredictionInfos/:queryId/:stageId/:DOP", Routes::bind(&StatsEndpoint::getQueryStagePredictionInfosExtern, this));
        Routes::Get(router, "/v1/query/getQueryBottleneckExtern/:queryId", Routes::bind(&StatsEndpoint::getQueryBottleneckExtern, this));
        Routes::Get(router, "/v1/query/getQueryBottleneckStagesAndAnalyzeExterns/:queryId/:factor", Routes::bind(&StatsEndpoint::getQueryBottleneckStagesAndAnalyzeExterns, this));



        Routes::Get(router, "/v1/sql/runSqlFromWebExtern/:sql", Routes::bind(&StatsEndpoint::runSqlFromWebExtern, this));



        Routes::Post(router, "/v1/cluster/reportHeartbeat/:infoLength/:info", Routes::bind(&StatsEndpoint::reportHeartbeat, this));


        Routes::Get(router, "/v1/welcome", Routes::bind(&StatsEndpoint::getIndexHtml, this));


    }

    void doRecordMetric(const Rest::Request& request, Http::ResponseWriter response) {
        auto name = request.param(":name").as<std::string>();

        Guard guard(metricsLock);
    }



    string getRawValue(const Rest::Request& request,string length,string attribute)
    {
        Base64 base64;

        string valueLength = request.param(length).as<std::string>();

        string attributeValue;
        if (request.hasParam(attribute)) {
            auto value = request.param(attribute);
            attributeValue = value.as<string>();
        }
        unsigned char *decode_Value = (unsigned char *)malloc(BASE64_DECODE_OUT_SIZE(attributeValue.size()));
        base64.base64_decode(attributeValue.c_str(),attributeValue.size(),decode_Value);
        string rawValue((char*)decode_Value,atoi(valueLength.c_str()));
        free(decode_Value);

        return rawValue;

    }
    void getIndexHtml(const Rest::Request& request, Http::ResponseWriter response) {

        Http::serveFile(response,"Front/index.html");

    }

    void updateTask(const Rest::Request& request, Http::ResponseWriter response) {


        string taskName = getRawValue(request,":taskIdLength",":taskId");
        string update = getRawValue(request,":updateRequest_Length",":updateRequest");

        TaskServerInterFace api;
        spdlog::debug("server updateTask in");
        TaskInfo taskResponse = api.createOrUpdateTask(taskName,update);
        spdlog::debug("server updateTask out");


        response.send(Http::Code::Ok,TaskInfo::Serialize(taskResponse));


    }

    void getTaskInfo(const Rest::Request& request, Http::ResponseWriter response) {


        string taskName = getRawValue(request,":taskIdLength",":taskId");

        TaskServerInterFace api;
        TaskInfo taskResponse = api.getTaskInfo(taskName);


        response.send(Http::Code::Ok,TaskInfo::Serialize(taskResponse));

    }

    void closeTask(const Rest::Request& request, Http::ResponseWriter response) {


        string taskName = getRawValue(request,":taskIdLength",":taskId");

        TaskServerInterFace api;
        TaskInfo taskResponse = api.closeTask(taskName);

        response.send(Http::Code::Ok,TaskInfo::Serialize(taskResponse));

    }




    void getTaskInfoExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string taskId;
        if (request.hasParam(":taskId")) {
            auto value = request.param(":taskId");
            taskId = value.as<string>();
        }

        TaskServerInterFace api;
        TaskInfo taskResponse = api.getTaskInfo(taskId);

        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,TaskInfo::Serialize(taskResponse));

    }

    void getAllTaskInfoExtern(const Rest::Request& request, Http::ResponseWriter response) {


        TaskServerInterFace api;
        vector<TaskInfo> taskResponse = api.getAllTaskInfo();
        nlohmann::json responseString;

        string resultString;

        resultString.append("[");
        for(auto tr : taskResponse)
        {
           resultString.append(tr.Visualization(tr));
           resultString.append(",");
        }
        resultString.pop_back();
        resultString.append("]");

        cout << resultString << endl;

        nlohmann::json  re = nlohmann::json::parse(resultString);

        string result = re.dump(2);

        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,result);

    }

    void getAllRunningQueryInfoExtern(const Rest::Request& request, Http::ResponseWriter response) {


        QueryInterFace api;
        string result = api.getAllRunningQueryInfo();

        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,result);

    }

    void getAllQueryInfoExtern(const Rest::Request& request, Http::ResponseWriter response) {


        QueryInterFace api;
        string result = api.getAllQueryInfo();

        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,result);

    }

    void listAllQueriesExtern(const Rest::Request& request, Http::ResponseWriter response) {


        QueryInterFace api;
        string result = api.getAllRegQueries();

        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,result);

    }


    void giveMeAQueryExtern(const Rest::Request& request, Http::ResponseWriter response) {

        string queryId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }

        QueryInterFace api;
        api.give_me_a_query(queryId);

        string result = "Request Submiited!";
        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,result);

    }


    void getQueryInfoExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.getQueryInfo(queryId);
        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }


    void getQueryResultExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.getQueryResult(queryId);
        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }

    void getQueryThroughputsExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.getQueryThroughputs(queryId);
        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }

    void getQueryThroughputsInfoExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.getQueryThroughputsInfo(queryId);
        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }

    void addStageConcurrentExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        string stageId;

        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }
        if (request.hasParam(":stageId")) {
            auto value = request.param(":stageId");
            stageId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.addStageTask(queryId,atoi(stageId.c_str()));
        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }

    void decreaseStageParallelismExtern(const Rest::Request& request, Http::ResponseWriter response) {

        string queryId;
        string stageId;

        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }
        if (request.hasParam(":stageId")) {
            auto value = request.param(":stageId");
            stageId = value.as<string>();
        }


        QueryInterFace api;
        string taskResponse = api.decreaseStageParallelism(queryId,stageId);

        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }

    void addStageTaskGroupConcurrentExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        string stageId;
        string taskNum;

        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }
        if (request.hasParam(":stageId")) {
            auto value = request.param(":stageId");
            stageId = value.as<string>();
        }

        if (request.hasParam(":taskNum")) {
            auto value = request.param(":taskNum");
            taskNum = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.addStageTaskGroup(queryId,atoi(stageId.c_str()),atoi(taskNum.c_str()));
        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }

    void addStageAllTaskPipelineConcurrentExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        string stageId;
        string pipelineId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }
        if (request.hasParam(":stageId")) {
            auto value = request.param(":stageId");
            stageId = value.as<string>();
        }
        if (request.hasParam(":pipelineId")) {
            auto value = request.param(":pipelineId");
            pipelineId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.addStageTaskPipeline(queryId,stageId,pipelineId);



        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }
    void subStageAllTaskPipelineConcurrentExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        string stageId;
        string pipelineId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }
        if (request.hasParam(":stageId")) {
            auto value = request.param(":stageId");
            stageId = value.as<string>();
        }
        if (request.hasParam(":pipelineId")) {
            auto value = request.param(":pipelineId");
            pipelineId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.subStageTaskPipeline(queryId,stageId,pipelineId);



        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }


    void getQueryBottleneckExtern(const Rest::Request& request, Http::ResponseWriter response) {

        string queryId;
        string stageId;

        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.getQueryBottlenecks(queryId);
        nlohmann::json responseString;

        string resultString;


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }
    void  getQueryBottleneckStagesAndAnalyzeExterns(const Rest::Request& request, Http::ResponseWriter response) {

        string queryId;
        string factor;

        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }
        if (request.hasParam(":factor")) {
            auto value = request.param(":factor");
            factor = value.as<string>();
        }


        QueryInterFace api;
        string taskResponse = api.getQueryBottlenecksAndAnalyze(queryId,factor);



        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }

    void getQueryStagePredictionInfosExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string queryId;
        string stageId;
        string pipelineId;
        if (request.hasParam(":queryId")) {
            auto value = request.param(":queryId");
            queryId = value.as<string>();
        }
        if (request.hasParam(":stageId")) {
            auto value = request.param(":stageId");
            stageId = value.as<string>();
        }
        if (request.hasParam(":DOP")) {
            auto value = request.param(":DOP");
            pipelineId = value.as<string>();
        }

        QueryInterFace api;
        string taskResponse = api.subStageTaskPipeline(queryId,stageId,pipelineId);



        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,taskResponse);

    }


    void runSqlFromWebExtern(const Rest::Request& request, Http::ResponseWriter response) {


        string cmd;

        if (request.hasParam(":sql")) {
            auto value = request.param(":sql");
            cmd = value.as<string>();
        }


        string cmdResponse;
        if(this->shell != NULL)
            cmdResponse = this->shell->shellCommandsForWeb(cmd);
        else
            cmdResponse = "Not ready yet.";


        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,cmdResponse);

    }


    void reportHeartbeat(const Rest::Request& request, Http::ResponseWriter response) {



        string heartbeat = getRawValue(request,":infoLength",":info");
        ClusterServer::resolveHeartbeat(heartbeat);

        response.headers().add<Http::Header::AccessControlAllowOrigin>("*");
        response.send(Http::Code::Ok,"ok");

    }






};

shared_ptr<StatsEndpoint> StartHttpServer()
{
   // taskServer::init();



    WebConfig config;
    Port port(atoi(config.getWebServerPort().c_str()));

    int thr = 2;



    Address addr(Ipv4::any(), port);

    string info;
    info+="RestFulServer Running. ";
    info+="Cores = "+to_string(hardware_concurrency());
    info+=". Using "+ to_string(thr);
    info+=" threads.";

    spdlog::info(info);


    shared_ptr<StatsEndpoint> stats = make_shared<StatsEndpoint>(addr);

    thread process([stats,thr] {

        stats->init(thr);
        stats->start();
    });
    process.detach();

    return stats;


}



#endif //OLVP_SERVER_HPP
