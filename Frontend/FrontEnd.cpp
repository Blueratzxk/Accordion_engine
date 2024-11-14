//
// Created by zxk on 11/9/24.
//


#include "FrontEnd.h"

#include "pg_query.h"

#include "AstNodes/AstNodeAllocator.hpp"
#include "Planner/Expression/RowExpressionNodeAllocator.hpp"
#include "AstParser.hpp"
#include "SqlParser.hpp"
#include "AstViewer.hpp"
#include "AstCleaner.hpp"
#include "Analyzer/StatementAnalyzer.hpp"

#include "Planner/RelationPlanner.h"

#include "PlanNode/PlanNodeTreeCleaner.hpp"


#include "Optimizer/Optimizer.hpp"



void transformNodes(PlanNode* node)
{
    node->transform();
    auto childs = node->getSources();

    for(auto child : childs)
    {
        transformNodes(child);
    }
}


PlanNode* FrontEnd::go(string sql)
{

    spdlog::info(sql);
    SqlParser sqlParser(sql);
    sqlParser.parse();


    AstParser parser(sqlParser.getParseResultJson());
    spdlog::info(sqlParser.getParseResultJson());



    auto re = parser.parse();

    spdlog::info("==================AST=====================");
    for(auto re : parser.getAstExceptionCollector()->getInfos())
    {
        spdlog::info(re);
    }


    for(auto re : parser.getAstExceptionCollector()->getErrors())
    {
        spdlog::error(re);
    }

    for(auto error: parser.getAstExceptionCollector()->getErrors())
        this->feedbacks.push_back(error);


    if(re != NULL && parser.getAstExceptionCollector()->getErrors().size() == 0) {


        AstNodeViewer viewer;
        if (re != NULL)
            viewer.Visit(re, NULL);


        shared_ptr<StatementAnalyzer> statementAnalyzer = make_shared<StatementAnalyzer>(re);
        statementAnalyzer->analyze();


        for(auto error: statementAnalyzer->getExceptionCollector()->getErrors())
            this->feedbacks.push_back(error);
        for(auto error: statementAnalyzer->getExceptionCollector()->getWarns())
            this->feedbacks.push_back(error);

        spdlog::info("==================Analyzer=====================");
        auto warns = statementAnalyzer->getExceptionCollector()->getWarns();
        for (auto e: warns) {
            spdlog::warn(e);
        }

        auto exceptions = statementAnalyzer->getExceptionCollector()->getErrors();
        for (auto e: exceptions) {
            spdlog::error(e);
        }

        shared_ptr<RelationPlan> planResult;
        shared_ptr<PlanNodeAllocator> planNodeAllocator = make_shared<PlanNodeAllocator>();
        shared_ptr<RelationPlanner> relationPlanner;
        shared_ptr<Optimizer> optimizer;

        PlanNode *planRoot = NULL;
        if (exceptions.size() == 0) {
            relationPlanner = make_shared<RelationPlanner>(statementAnalyzer->getAnalysis(), planNodeAllocator);
            planResult = shared_ptr<RelationPlan>((RelationPlan *) relationPlanner->Visit(re, NULL));

            for(auto error: relationPlanner->getExceptionCollector()->getErrors())
                this->feedbacks.push_back(error);
            for(auto error: relationPlanner->getExceptionCollector()->getWarns())
                this->feedbacks.push_back(error);

            if (relationPlanner->getExceptionCollector()->getErrors().empty()) {
                optimizer = make_shared<Optimizer>(statementAnalyzer->getAnalysis(),relationPlanner->getPlanNodeIdAllocator(),planNodeAllocator);
                planRoot = optimizer->optimize(planResult->getRoot());
            }

        }


        parser.releaseAstNodes();
        if (relationPlanner != NULL)
            relationPlanner->releaseAstNodes();

        if(planRoot != NULL) {
            planNodeAllocator->carvePlanTree(planRoot);
            transformNodes(planRoot);
        }
        planNodeAllocator->release_all();






        return planRoot;
    }
    return NULL;

}



PlanNode* FrontEnd::gogogo(string sql) {



    //  string sql = "select count(l_shipdate),count(l_partkey) from lineitem,part,nation where l_extendedprice +1.3 > 3";

    //   string sql = "select count(l_partkey) from lineitem natural join nation";

  //  string sql = "select l_suppkey,s_suppkey,sum(l_suppkey),sum(l_extendedprice),count(s_suppkey) from lineitem join (nation join supplier on n_nationkey = s_nationkey) on l_suppkey = s_suppkey"
  //               " where s_acctbal+1.2 > 10 group by l_suppkey,s_suppkey";

//    string sql = "select count(l_suppkey) from lineitem join (nation join supplier on n_nationkey = s_nationkey and n_nationkey = s_nationkey) on l_suppkey = s_suppkey"
    //                  " where s_suppkey+1.2 > 10";

//    string sql = "select count(l_partkey),count(l_orderkey) from lineitem where l_partkey+l_extendedprice > 3 or l_partkey < 13 group by l_partkey";

   //sql = "select count(*) from supplier join nation on s_suppkey=n_nationkey";

    return go(sql);


   // std::cout << "Hello, World!" << std::endl;

}

std::list<std::string> FrontEnd::getFeedbacks() {
    return  this->feedbacks;
}

