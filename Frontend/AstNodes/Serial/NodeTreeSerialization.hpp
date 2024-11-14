//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_NODETREESERIALIZATION_HPP
#define OLVP_NODETREESERIALIZATION_HPP




#include "../tree.h"
#include "../AstNodeVisitor.h"
#include "../DefaultAstExpressionVisitor.hpp"
#include "nlohmann/json.hpp"
#include "nlohmann/fifo_map.hpp"

#include "../AstNodePtr.hpp"

using namespace nlohmann;

template<class K, class V, class dummy_compare, class A>
using my_workaround_fifo_map = fifo_map<K, V, fifo_map_compare<K>, A>;
using order_json = basic_json<my_workaround_fifo_map>;



class AstNodeTreeSerializer:public DefaultAstExpressionVisitor {


public:


    void *VisitFunctionCall(FunctionCall *node, void *context) {

        order_json childs = order_json::array();

        vector < Node * > arguments = node->getChildren();
        for (int i = 0; i < arguments.size(); i++) {
            order_json  *child = (order_json*)Visit(arguments[i], NULL);
            childs.push_back(*child);
            delete child;
        }



        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["functionName"] = node->getFuncName();
        NodeAttrbutes["outputType"] = node->getOutputType();

        NodeAttrbutes["location"] = node->getLocation();
        (*result)["FunctionCall"] = NodeAttrbutes;
        (*result)["childs"] = childs;



        return result;

    }

    void *VisitDoubleLiteral(DoubleLiteral *node, void *context) {


        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = to_string(node->getValue());
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["DoubleLiteral"] = NodeAttrbutes;


        return result;

    }

    void *VisitInt32Literal(Int32Literal *node, void *context) {

        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = to_string(node->getValue());
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["Int32Literal"] = NodeAttrbutes;


        return result;

    }

    void *VisitDate32Literal(Date32Literal *node, void *context) {

        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = node->getValue();
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["Date32Literal"] = NodeAttrbutes;


        return result;

    }

    void *VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral *node, void *context) {

        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = node->getValue();
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["DayTimeIntervalLiteral"] = NodeAttrbutes;


        return result;

    }


    void *VisitInt64Literal(Int64Literal *node, void *context) {

        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = to_string(node->getValue());
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["Int64Literal"] = NodeAttrbutes;


        return result;

    }

    void *VisitStringLiteral(StringLiteral *node, void *context) {

        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = node->getValue();
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["StringLiteral"] = NodeAttrbutes;


        return result;

    }

    void *VisitIdentifier(Identifier *node, void *context) {

        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = node->getValue();
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["Identifier"] = NodeAttrbutes;


        return result;

    }

    void *VisitColumn(Column *node, void *context) {

        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["value"] = node->getValue();
        NodeAttrbutes["columnType"] = node->getColumnType();
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["Column"] = NodeAttrbutes;


        return result;

    }

    void *VisitIfExpression(IfExpression *node, void *context)
    {

        order_json childs = order_json::array();

        vector < Node * > arguments = node->getChildren();
        for (int i = 0; i < arguments.size(); i++) {
            order_json  *child = (order_json*)Visit(arguments[i], NULL);
            childs.push_back(*child);
            delete child;
        }


        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["outputType"] = node->getOutputType();
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["IfExpression"] = NodeAttrbutes;
        (*result)["childs"] = childs;



        return result;


    }

    void *VisitInExpression(InExpression *node, void *context)
    {

        order_json childs = order_json::array();

        vector < Node * > arguments = node->getChildren();
        for (int i = 0; i < arguments.size(); i++) {
            order_json  *child = (order_json*)Visit(arguments[i], NULL);
            childs.push_back(*child);
            delete child;
        }


        order_json NodeAttrbutes;

        order_json *result = new order_json();


        NodeAttrbutes["inputType"] = node->getInputType();
        NodeAttrbutes["inConstants"] = node->getInConstants();
        NodeAttrbutes["location"] = node->getLocation();
        (*result)["InExpression"] = NodeAttrbutes;
        (*result)["childs"] = childs;



        return result;


    }


    string Serialize(Node *root)
    {
        order_json *output = (order_json*)this->Visit(root,NULL);
        string s = output->dump(2);
        delete(output);
        return s;
    }

    string Serialize(std::shared_ptr<AstNodePtr> root)
    {
        order_json *output = (order_json*)this->Visit(root->get(),NULL);
        string s = output->dump(2);
        delete(output);
        return s;
    }

    string test()
    {
        Int64Literal *suppkeyValue = new Int64Literal("0","123");
        Column *col = new Column("0","s_suppkey","int64");

        FunctionCall *funcName = new FunctionCall("0","multiply","int64");
        funcName->addChilds({col,suppkeyValue});


        StringLiteral *snameValue = new StringLiteral("0","_zxk");
        FunctionCall *funcName2 = new FunctionCall("0","concat","string");
        funcName2->addChilds({funcName,snameValue});

        Column *colNation = new Column("0","s_suppkey","int64");

        FunctionCall *funcName3 = new FunctionCall("0","multiply","int64");
        funcName3->addChilds({colNation,funcName2});

        Column *colNation2 = new Column("0","s_suppkey","int64");
        Column *colSupp2 = new Column("0","s_nationkey","int64");
        FunctionCall *funcName4 = new FunctionCall("0","subtract","int64");
        funcName4->addChilds({colNation2,colSupp2});


        FunctionCall *funcName5 = new FunctionCall("0","subtract","int64");
        funcName5->addChilds({funcName4,funcName3});

        return this->Serialize(funcName5);


    }


};




#endif //OLVP_NODETREESERIALIZATION_HPP
