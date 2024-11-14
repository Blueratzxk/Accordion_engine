//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_QUERYSERVER_HPP
#define OLVP_QUERYSERVER_HPP



#include "../Query/QueryManager.hpp"
#include "../Script/ScriptExecutor.hpp"
#include "../System/ClusterServer.h"
class QueryServer
{
public:
    static shared_ptr<QueryManager> queryServer;
    static shared_ptr<ScriptExecutor> scriptExecutor;

    static shared_ptr<ScriptExecutor> getScriptExecutor()
    {
        if(QueryServer::scriptExecutor == NULL)
            QueryServer::scriptExecutor = make_shared<ScriptExecutor>(QueryServer::queryServer);
        return QueryServer::scriptExecutor;
    }

    static shared_ptr<QueryManager> getQueryServer() {

        return QueryServer::queryServer;
    }

};

shared_ptr<QueryManager> QueryServer::queryServer = make_shared<QueryManager>();
shared_ptr<ScriptExecutor> QueryServer::scriptExecutor = NULL;



#endif //OLVP_QUERYSERVER_HPP
