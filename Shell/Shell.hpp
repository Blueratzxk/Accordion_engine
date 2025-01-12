//
// Created by zxk on 7/5/24.
//

#ifndef OLVP_SHELL_HPP
#define OLVP_SHELL_HPP


#include "../Frontend/FrontEnd.h"

class Shell
{
    bool shellStatus = false;

    shared_ptr<ScriptExecutor> scriptExecutor = NULL;
public:

    Shell()
    {
        WebConfig config;
        if(config.thisNodeIsCoordinator())
            this->shellStatus = true;

        scriptExecutor = QueryServer::getScriptExecutor();
    }

    void start()
    {
        ExecutionConfig config;
        string logLevel = config.getLog_level();

        if(logLevel == "debug") {
            spdlog::set_level(spdlog::level::debug);
            spdlog::info("LOG_LEVEL:debug");
        }
        else if(logLevel == "info") {
            spdlog::set_level(spdlog::level::info);
            spdlog::info("LOG_LEVEL:info");
        }
        else if(logLevel == "warn") {
            spdlog::set_level(spdlog::level::warn);
            spdlog::info("LOG_LEVEL:warn");
        }
        else if(logLevel == "critical") {
            spdlog::set_level(spdlog::level::critical);
            spdlog::info("LOG_LEVEL:critical");
        }
        else {
            spdlog::set_level(spdlog::level::info);
            spdlog::info("LOG_LEVEL:info");
        }




        if(!this->shellStatus)
        {
            while(1)
                sleep(INT_MAX);
        }

        //shared_ptr<ScriptExecutor> scriptExecutor = make_shared<ScriptExecutor>(QueryServer::queryServer);

        auto taskServerInfos = TaskServer::getTaskServer();
        shared_ptr<CpuInfoCollector> cpuInfoCollector = TaskServer::getCpuInfoCollector();

        spdlog::info("Press 'Enter' to enter the shell.");
        sync();
        getchar();

        for(;;){



            cout << "Shell >";
            string cmd;
            getline(cin, cmd);

            string run("run ");
            if(cmd.compare(0,run.length(),run) == 0)
            {
                int offset = cmd.length() - run.length();
                if(offset <= 0) {
                    spdlog::error("Need Query Name.");
                    continue;
                }
                string queryName = cmd.substr(run.length(),offset);
                if(!scriptExecutor->isRunning()) {
                    thread(&ScriptExecutor::executeByCommand,scriptExecutor,queryName).detach();
                }
                else
                    spdlog::info("Script is running!");

                continue;
            }
            if(cmd == "s")
            {
                if(!scriptExecutor->isRunning()) {
                    thread(&ScriptExecutor::execute,scriptExecutor).detach();
                }
                else
                    spdlog::info("Script is running!");
            }
            else if(cmd == "tasks")
            {
                vector<TaskInfo> taskinfos = taskServerInfos->getAllTaskInfo();
                for(auto taskinfo : taskinfos){
                    spdlog::info(taskinfo.getTaskInfoDescriptor()->ToString());
                }
            }
            else if(cmd == "sheart")
            {
                ClusterServer::getNodesManager()->showHeartbeat();
            }
            else if(cmd == "hheart")
            {
                spdlog::info("OK, now hide heartbeats infos.");
                ClusterServer::getNodesManager()->hideHeartbeat();
            }
            else if(cmd == "schemas")
            {
                CatalogsMetaManager catalogsMetaManager;
                catalogsMetaManager.viewSchemas();
            }
       //     else if(cmd == "tu") //thread usages
       //     {
       //         auto allRunningTaskContexts = taskServerInfos->getAllRunningTaskContexts();
       //         for(auto q : allRunningTaskContexts)
       //             for(auto tc : q.second)
       //                 cpuInfoCollector->gatherTids(TaskId::Serialize(*(tc->getTaskId())),tc->getAllTaskTids());
//
      //          cpuInfoCollector->computeUsages();
      //      }
            else if(cmd == "u") //cpu usages
            {
                cpuInfoCollector->computeNodeCpuUsage();
            }
            else if(cmd == "pu") //process cpu usages
            {
                cpuInfoCollector->computeProcessUsage();
            }
            else if(cmd == "ru") //show cpu usages refresh
            {
                cpuInfoCollector->openRefresh();
            }
            else if(cmd == "qu") //show cpu usages refresh
            {
                QueryServer::queryServer->showAllQueryCpuUsage();
            }
            else if(cmd == "cu") //close cpu usages refresh
            {
                cpuInfoCollector->closeRefresh();
            }
            else if(cmd == "su") //show usage once
            {
                cpuInfoCollector->showCurrentUsages();
            }
            else if(cmd == "showSche")
            {
                spdlog::info("OK, now show scheduled nodes infos.");
                ClusterServer::getNodesManager()->openOutputScheduleNodesLog();
            }
            else if(cmd == "closeSche")
            {
                spdlog::info("OK, now hide scheduled nodes infos.");
                ClusterServer::getNodesManager()->closeOutputScheduleNodesLog();
            }
            else if(cmd == "n")
            {
                ClusterServer::getNodesManager()->displayAllNodesInfo();
            }
            else if(cmd == "rn")
            {

                ClusterServer::openClusterInfoDisplay();
            }
            else if(cmd == "cn")
            {
                ClusterServer::closeClusterInfoDisplay();
            }
            else if(cmd == "dag")
            {
                QueryServer::getQueryServer()->detectBottleneckForQuerys();
            }

            else if(cmd == "v")
            {
                QueryServer::getQueryServer()->viewTaskInfoGroupByNodes();
            }
            else if(cmd == "p")
            {
                if(QueryServer::getQueryServer()->getPPM()->isStart())
                {
                    QueryServer::getQueryServer()->getPPM()->show();
                }
            }
            else if(cmd == "pt")
            {
                if(QueryServer::getQueryServer()->getPPM()->isStart()) {
                    QueryServer::getQueryServer()->getPPM()->test();
                }
            }
            else if(cmd == "h")
            {
                spdlog::info("run 'query name' ---> run a tpch query.");
                spdlog::info("v                ---> show all running tasks grouped by the cluster node.");
                spdlog::info("s                ---> run script.");
                spdlog::info("qu               ---> show all queries' CPU usage.");
                spdlog::info("n                ---> show all nodes' informations.");
                spdlog::info("rn/cn            ---> periodically display(or close) the nodes' informations.");
                spdlog::info("schemas          ---> show the schema in the configuration file.");

            }
            else if(cmd == "sql") {

                FrontEnd frontend;
                try {
                    auto root = frontend.gogogo("select count(*) from lineitem");
                    QueryServer::getQueryServer()->ExecuteQuery("select count(*) from lineitem","test",root);
                }
                catch (exception &e)
                {
                    spdlog::error("Sql error! "+ string(e.what()));
                }
            }


        }



    }


    std::string subreplace(std::string resource_str, std::string sub_str, std::string new_str)
    {
        std::string dst_str = resource_str;
        std::string::size_type pos = 0;
        while((pos = dst_str.find(sub_str)) != std::string::npos)   //替换所有指定子串
        {
            dst_str.replace(pos, sub_str.length(), new_str);
        }
        return dst_str;
    }

    string shellCommandsForWeb(string cmd)
    {

        std::string::iterator end_pos = std::remove(cmd.begin(), cmd.end(), ' ');
        cmd.erase(end_pos, cmd.end());

        cmd = subreplace(cmd,"%20"," ");
        cmd = subreplace(cmd,"%3E",">");
        cmd = subreplace(cmd,"%3C","<");


        CatalogsMetaManager catalogsMetaManager;


        string run("run ");
        if(cmd.compare(0,run.length(),run) == 0)
        {
            int offset = cmd.length() - run.length();
            if(offset <= 0) {
                return "Need Query Name.";

            }
            string queryName = cmd.substr(run.length(),offset);
            if(!scriptExecutor->isRunning()) {
                thread(&ScriptExecutor::executeByCommand,scriptExecutor,queryName).detach();

                return QueryServer::getQueryServer()->getQuerySql(queryName);
            }
            else
                return "Script is running!";
        }

        string show("show");
        if(cmd == show)
        {

            auto tables = catalogsMetaManager.showTables();
            string tableString;
            tableString+="\n";
            for(auto table : tables)
            {
                tableString += (table+"\n");
            }
            return tableString;
        }

        string desc("desc ");
        if(cmd.compare(0,desc.length(),desc) == 0)
        {
            int offset = cmd.length() - desc.length();
            if(offset <= 0) {
                return "Need table Name.";

            }
            string tableName = cmd.substr(desc.length(),offset);
            return catalogsMetaManager.describeTable(tableName);
        }


        FrontEnd frontend;
        string feedback = "";
        PlanNode *root = NULL;
        try {

            root = frontend.gogogo(cmd);

            auto infos = frontend.getFeedbacks();
            if(!infos.empty())
                feedback.append("\n");
            for(auto info : infos)
                feedback.append(info+"\n");

            if(root != NULL) {
                QueryServer::getQueryServer()->ExecuteQuery(cmd,"QUERY", root);

                if(feedback != "")
                    return feedback+"Query Submitted!";

                return "Query Submitted!";
            }



        }
        catch (exception &e)
        {
            spdlog::error("Sql error! "+ string(e.what()));
        }

        if(feedback != "")
            return feedback;
        
        return "Unknown sentence.";


    }



};



#endif //OLVP_SHELL_HPP
