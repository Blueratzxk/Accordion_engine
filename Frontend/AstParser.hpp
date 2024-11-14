//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_ASTPARSER_HPP
#define FRONTEND_ASTPARSER_HPP

#include "SqlParser.hpp"
#include "AstNodes/Node.h"
#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"
#include "AstNodes/tree.h"
#include "AstNodes/AstExceptionCollector.hpp"
#include "AstNodes/JoinCriteria/JoinOn.hpp"
class AstParser
{
    char *SQL;
    Node *root;
    shared_ptr<AstExceptionCollector> astExceptionCollector;
    shared_ptr<AstNodeAllocator> astAllocator;
public:
    AstParser(char *SQL)
    {
        this->SQL = SQL;
        this->astExceptionCollector = make_shared<AstExceptionCollector>();
        this->astAllocator = make_shared<AstNodeAllocator>();
    }

    AstParser(char *SQL, shared_ptr<AstNodeAllocator> astNodeAllocator)
    {
        this->SQL = SQL;
        this->astExceptionCollector = make_shared<AstExceptionCollector>();
        this->astAllocator = astNodeAllocator;
    }


    Relation *genJoinTreeFromList(list<Relation*> nodes)
    {
        list<Relation*> temp;
        temp = nodes;

        while(temp.size() > 1)
        {
            auto *right = (Relation *)temp.back();
            temp.pop_back();
            auto left = (Relation *)temp.back();
            temp.pop_back();




            auto join = astAllocator->new_Join("0",Join::Type::INNER,left,right);
            temp.push_back(join);
        }
        return temp.front();
    }

    void releaseAstNodes()
    {
        this->astAllocator->release_all();
    }


    shared_ptr<AstExceptionCollector> getAstExceptionCollector()
    {
        return this->astExceptionCollector;
    }

    Node * parse()
    {
        nlohmann::json result = nlohmann::json::parse(SQL);
        nlohmann::json stmt = result["stmts"];
        if(stmt.size() > 1)
        {
            this->astExceptionCollector->recordError("Cannot parse multi-query at once!");
            return NULL;
        }
        else if(stmt.size() == 0)
        {

            this->astExceptionCollector->recordError("No statement found !");

            return NULL;
        }
        return parseStmt(stmt[0]);
    }


    Node *parseStmt(nlohmann::json stmt)
    {
        auto statement = stmt["stmt"];
       // spdlog::info(stmt.dump());

        if(!statement.contains("SelectStmt"))
        {
            this->astExceptionCollector->recordError("Only support SelectStmt now !");
            return NULL;
        }

        return astAllocator->new_Query("0",parseSelectStmt(statement["SelectStmt"]));
    }

    //A_Expr is "> < = !="     Bool_Expr is "and or ..."

    QueryBody *parseSelectStmt(nlohmann::json selectStmt)
    {

       // spdlog::info(selectStmt.dump());
        auto select = parseTargetList(selectStmt["targetList"]);

        auto from = parseFromClause(selectStmt["fromClause"]);

        auto where = parseWhereClause(selectStmt["whereClause"]);

        auto groupBy = parseGroupClause(selectStmt["groupClause"]);

        auto having = parseHavingClause(selectStmt["havingClause"]);

        auto sort = parseSortClause(selectStmt["sortClause"]);

        auto limitOffset = parseLimitOffset(selectStmt["limitOffset"]);

        auto limitCount = parseLimitCount(selectStmt["limitCount"]);

        if(from == NULL)
            this->astExceptionCollector->recordError("Unsupported query type!");

        return astAllocator->new_QuerySpecification("0",select,from,where,groupBy,NULL,NULL,NULL,"");
    }

    Select *parseTargetList(nlohmann::json targetList)
    {
        if(targetList.is_null())
            return NULL;


        list<SelectItem *> selectItems;
        this->astExceptionCollector->recordInfo(targetList.dump());

        for(auto resTarget : targetList)
        {
            auto res = parseResTarget(resTarget["ResTarget"]);
            if(res != NULL)
            selectItems.push_back(res);
        }
        return astAllocator->new_Select("0",selectItems);

    }

    SelectItem *parseResTarget(nlohmann::json ResTarget)
    {
        if(ResTarget.is_null())
            return NULL;

        this->astExceptionCollector->recordInfo(ResTarget.dump());

        return parseResTargetVal(ResTarget["val"]);
    }
    SelectItem *parseResTargetVal(nlohmann::json ResTargetVal) {

        if(ResTargetVal.is_null())
            return NULL;

        for (auto item: ResTargetVal.items()) {
            if (item.key() == "SubLink") {


                this->astExceptionCollector->recordError("Unsupported subquery in select now !");
                return NULL;
            }
            else if(item.key() == "FuncCall")
            {
                auto col = parseFunctionCall(ResTargetVal["FuncCall"]);
                if(col != NULL)
                    return astAllocator->new_SingleColumn("0",col);
                else
                    return NULL;
            }
            else if(item.key() == "ColumnRef")
            {
                auto col = parseColumnRef(ResTargetVal["ColumnRef"]);
                if(col != NULL)
                    return astAllocator->new_SingleColumn("0",col);
                else
                    return NULL;
            }
            else
            {
                this->astExceptionCollector->recordError("Unsupported key in select now ! -->" + item.key());

                return NULL;
            }
        }

        return NULL;

    }

    Expression *parseFunctionCall(nlohmann::json funcCall)
    {

        if(funcCall.is_null())
            return NULL;

        if(funcCall.contains("agg_star"))
        {
            this->astExceptionCollector->recordInfo("function name:* "+funcCall["funcname"].dump());

            string col = funcCall["funcname"][0]["String"]["sval"];
            for(int i = 1 ; i < funcCall["funcname"].size() ; i++)
                col+=("."+(string)(funcCall[i]["String"]["sval"]));


            return astAllocator->new_FunctionCall("0",col,{});
        }
        else if(funcCall.contains("args"))
        {
            vector<Expression*> args;
            for(auto item : funcCall["args"])
            {
               // args.push_back(parseColumnRef(item["ColumnRef"]));

               auto argNode = parseExpr(item);
                args.push_back(argNode);
                this->astExceptionCollector->recordInfo("function name: "+funcCall["funcname"].dump());
            }

            string col = funcCall["funcname"][0]["String"]["sval"];
            for(int i = 1 ; i < funcCall["funcname"].size() ; i++)
                col+=("."+(string)(funcCall["funcname"][i]["String"]["sval"]));

            return astAllocator->new_FunctionCall("0",col,args);
        }
        else
        {
            this->astExceptionCollector->recordError("Cannot find attributes in function call!" );
            return NULL;
        }


    }

    Relation *parseFromClause(nlohmann::json fromClause)
    {

        if(fromClause.is_null())
            return NULL;

        this->astExceptionCollector->recordInfo(fromClause.dump());

        int rangeVarCount = 0;

        list<Relation*> tables;
        for(auto fromItem : fromClause)
        {
            for(auto item : fromItem.items()) {
                if (item.key() == "RangeSubselect") {
                    this->astExceptionCollector->recordError("Unsupported subquery in from clause now !");
                    return NULL;
                }
                else if (item.key() == "JoinExpr") {


                    //this->astExceptionCollector->recordError("Unsupported join now !");
                    return this->parseJoinExpr(fromItem["JoinExpr"]);;
                }
                else if(item.key() == "RangeVar")
                {
                    tables.push_back(parseRangeVar(fromItem["RangeVar"]));
                    rangeVarCount++;
                }
                else
                {
                    this->astExceptionCollector->recordError("Unsupported key "+ item.key() + "!");
                    return NULL;
                }

            }
        }
        if(rangeVarCount > 1) {

            this->astExceptionCollector->recordInfo("Multi-table join!");
            return genJoinTreeFromList(tables);

        }
        else {
            this->astExceptionCollector->recordInfo("One table scan!");
            return tables.front();
        }
    }

    Expression *parseWhereClause(nlohmann::json whereClause)
    {

        if(whereClause.is_null())
            return NULL;

        //spdlog::info(whereClause.dump());

        for(auto item : whereClause.items()) {
            if(item.key() == "A_Expr")
            {
                return parseA_Expr(whereClause["A_Expr"]);
            }
            else if (item.key() == "BoolExpr")
            {
                return parseBoolExpr(whereClause["BoolExpr"]);
            }
            else
            {
                this->astExceptionCollector->recordError("Unsupported where clause "+ item.key() + "!");
                return NULL;
            }
        }

        return NULL;

    }

    Join *parseJoinExpr(nlohmann::json joinExpr)
    {

        if(joinExpr.is_null())
            return NULL;

        Expression *joinCriteria = NULL;
        shared_ptr<JoinCriteria> joinCriteriaObj;

        if(joinExpr.contains("quals"))
        {
            joinCriteria = parseExpr(joinExpr["quals"]);
            joinCriteriaObj = make_shared<JoinOn>(joinCriteria);
        }
        else if(joinExpr.contains("usingClause"))
        {
            this->astExceptionCollector->recordError("Unsupported using clause now!");
            return NULL;
        }
        else if(joinExpr.contains("isNatural"))
        {
            this->astExceptionCollector->recordError("Unsupported natural join now!");
            return NULL;
        }
        else
        {
            this->astExceptionCollector->recordError("Unsupported clause now!");
            return NULL;
        }

        string type = joinExpr["jointype"];


        Relation *larg,*rarg;

        larg = parseJoinArg(joinExpr["larg"]);

        rarg = parseJoinArg(joinExpr["rarg"]);



        Join *join = astAllocator->new_Join("0",Join::INNER,larg,rarg,joinCriteriaObj);

        //spdlog::info(whereClause.dump());

       // spdlog::info(joinExpr.dump());

        return join;

    }
    Relation *parseJoinArg(nlohmann::json joinArg) {

        if(joinArg.contains("JoinExpr"))
            return parseJoinExpr(joinArg["JoinExpr"]);
        else if(joinArg.contains("RangeVar"))
            return parseRangeVar(joinArg["RangeVar"]);

        this->astExceptionCollector->recordError("Unsupported join arg!"+joinArg.dump());
        return NULL;
    }

    Expression *parseBoolExpr(nlohmann::json boolExpr)
    {

        if(boolExpr.is_null())
            return NULL;

        nlohmann::json  args = boolExpr["args"];
        string boolop = boolExpr["boolop"];
        this->astExceptionCollector->recordInfo(boolop);

        list<Expression *> argExpressions;
        for(auto arg : args)
        {
            argExpressions.push_back(parseWhereClause(arg));
        }

        if(boolop == "NOT_EXPR")
        {
            return astAllocator->new_NotExpression("0",argExpressions.front());
        }
        else if(boolop == "AND_EXPR")
        {
            return astAllocator->new_LogicalBinaryExpression("0","AND",argExpressions.front(),argExpressions.back());
        }
        else if(boolop == "OR_EXPR")
        {
            return astAllocator->new_LogicalBinaryExpression("0","OR",argExpressions.front(),argExpressions.back());
        }
        else
        {
            this->astExceptionCollector->recordError("Unsupported logical operation "+ boolop + "!");
            return NULL;
        }


    }
    Expression *parseA_Expr(nlohmann::json a_Expr) {
        if (a_Expr.is_null())
            return NULL;

        nlohmann::json lexpr = a_Expr["lexpr"];
        nlohmann::json rexpr = a_Expr["rexpr"];



        auto left = parseExpr(lexpr);
        auto right = parseExpr(rexpr);


        string op = a_Expr["name"][0]["String"]["sval"];
                //[0]["string"]["sval"];
        this->astExceptionCollector->recordInfo(op);

        if (ComparisionExpression::isComparision(op))
            return astAllocator->new_ComparisionExpression("0", op, left, right);
        else if (ArithmeticBinaryExpression::isArithmetic(op))
            return astAllocator->new_ArithmeticBinaryExpression("0", op, left, right);
        else
        {
            this->astExceptionCollector->recordError("Unsupported operator "+op+"!");
            return NULL;
        }
    }

    Expression *parseExpr(nlohmann::json expr)
    {

        if(expr.is_null())
            return NULL;

        //spdlog::info("######"+expr.dump());
        for(auto item : expr.items()) {
            if (item.key() == "SubLink") {
                this->astExceptionCollector->recordError("Unsupported subquery in where clause now !");
                return NULL;
            }
            else if (item.key() == "ColumnRef") {
                return parseColumnRef(item.value());
            }
            else if (item.key() == "A_Const") {
                this->astExceptionCollector->recordInfo(item.value().dump());

                auto constValue = item.value();
                if(constValue.contains("ival"))
                {
                    if(constValue["ival"].empty())
                        return astAllocator->new_Int32Literal("0", "0");

                    int number = constValue["ival"]["ival"];
                    return astAllocator->new_Int32Literal("0", to_string(number));
                }
                else if(constValue.contains("fval"))
                {
                    string number = constValue["fval"]["fval"];
                    return astAllocator->new_DoubleLiteral("0", number);
                }
                else if(constValue.contains("String"))
                {
                    return astAllocator->new_StringLiteral("0",item.value()["String"]["sval"]);
                }
                else
                {
                    this->astExceptionCollector->recordError("Unsupported const type "+item.key() + "!");
                    return NULL;
                }

            }
            else if (item.key() == "A_Expr") {

                return parseA_Expr(item.value());
            }
            else if (item.key() == "BoolExpr") {

                return parseBoolExpr(item.value());
            }
            else
            {
                this->astExceptionCollector->recordError("Unsupported key "+ item.key() + "!");
                return NULL;
            }

        }
        return NULL;
    }


    Identifier *parseColumnRef(nlohmann::json ColumnRef)
    {

        if(ColumnRef.is_null())
            return NULL;

        nlohmann::json fields = ColumnRef["fields"];
        //spdlog::info(fields.dump());
        string col;

        int colNum = 0;

        for(auto field : fields)
        {
            for(auto item : field.items()) {
                if(item.key() != "String")
                {
                    this->astExceptionCollector->recordError("Unsupported parameter "+ item.key() +" now!");
                    return NULL;
                }
            }

            string f = field["String"]["sval"];
            if(colNum >= 1)
                col += ("."+f);
            else
                col = f;
            colNum++;
        }

        return astAllocator->new_Identifier("0",col,fields.size() > 1);
    }



    GroupBy *parseGroupClause(nlohmann::json groupClause)
    {
        if(groupClause.is_null())
            return NULL;



        list<Expression*> groupByItems;


        for(auto item : groupClause.items())
        {
            auto groupItem = this->parseExpr(item.value());
            groupByItems.push_back(groupItem);
        }
       // this->astExceptionCollector->recordError("Unsupported group clause now !");

        auto simpleGroupByExpressions = astAllocator->new_SimpleGroupBy("0",groupByItems);
        auto groupBy = astAllocator->new_GroupBy("0",false,{simpleGroupByExpressions});

        return groupBy;

    }

    Node *parseHavingClause(nlohmann::json havingClause)
    {
        if(havingClause.is_null())
            return NULL;

        this->astExceptionCollector->recordError("Unsupported having clause now !");
        return NULL;
    }

    Node *parseSortClause(nlohmann::json sortClause)
    {
        if(sortClause.is_null())
            return NULL;

        this->astExceptionCollector->recordError("Unsupported order by clause now !");
        return NULL;


    }

    Relation *parseRangeVar(nlohmann::json rangeVar)
    {
        if(rangeVar.is_null())
            return NULL;

        if(rangeVar.contains("relname")) {
            string tableName = rangeVar["relname"];
            this->astExceptionCollector->recordInfo("Table name: " + tableName);


            return astAllocator->new_Table("0",tableName);
        }
        else
            return NULL;
    }


    Node *parseLimitOffset(nlohmann::json limitOffset)
    {
        if(limitOffset.is_null())
            return NULL;

        this->astExceptionCollector->recordError("Unsupported offset now !");
        return NULL;
    }


    Node *parseLimitCount(nlohmann::json limitCount)
    {
        if(limitCount.is_null())
            return NULL;

        this->astExceptionCollector->recordError("Unsupported limit now !");
        return NULL;
    }


};


#endif //FRONTEND_ASTPARSER_HPP
