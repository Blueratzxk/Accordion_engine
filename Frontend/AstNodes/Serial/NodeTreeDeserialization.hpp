//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_NODETREEDESERIALIZATION_HPP
#define OLVP_NODETREEDESERIALIZATION_HPP



#include "nlohmann/json.hpp"
#include "nlohmann/fifo_map.hpp"

#include "../tree.h"
#include "../AstNodePtr.hpp"

using namespace nlohmann;

template<class K, class V, class dummy_compare, class A>
using my_workaround_fifo_map = fifo_map<K, V, fifo_map_compare<K>, A>;
using order_json = basic_json<my_workaround_fifo_map>;




class AstNodeDeserializer
{
    typedef Node*  (AstNodeDeserializer::*Fun_ptr)(json node);
    map<string, Fun_ptr> funcMap;


public:
    AstNodeDeserializer(){
        initFuctions();
    }

    void initFuctions()
    {
        funcMap.insert(make_pair("Column", &AstNodeDeserializer::getColumn));
        funcMap.insert(make_pair("FunctionCall", &AstNodeDeserializer::getFunctionCall));
        funcMap.insert(make_pair("Identifier", &AstNodeDeserializer::getIdentifier));
        funcMap.insert(make_pair("DoubleLiteral", &AstNodeDeserializer::getDoubleLiteral));
        funcMap.insert(make_pair("Int32Literal", &AstNodeDeserializer::getInt32Literal));
        funcMap.insert(make_pair("Int64Literal", &AstNodeDeserializer::getInt64Literal));
        funcMap.insert(make_pair("StringLiteral", &AstNodeDeserializer::getStringLiteral));
        funcMap.insert(make_pair("Date32Literal", &AstNodeDeserializer::getDate32Literal));
        funcMap.insert(make_pair("DayTimeIntervalLiteral", &AstNodeDeserializer::getDayTimeIntervalLiteral));
        funcMap.insert(make_pair("IfExpression", &AstNodeDeserializer::getIfExpression));
        funcMap.insert(make_pair("InExpression", &AstNodeDeserializer::getInExpression));
    }


    Node *deserialize(string nodeName,json node)
    {

        if (funcMap.count(nodeName))
        {
            return  (this->*funcMap[nodeName])(node);
        }
        else
        {
            cout << "nodeDeserializer cannot find the node "<<nodeName << "!"<<endl;
            exit(0);
        }
    }

    Node* getColumn(json node) {

        return new Column(node["location"],node["value"],node["columnType"]);
    }
    Node* getFunctionCall(json node) {

        return new FunctionCall(node["location"],node["functionName"], node["outputType"]);
    }
    Node* getIfExpression(json node) {


        return new IfExpression(node["location"], node["outputType"]);
    }
    Node* getInExpression(json node) {

        return new InExpression(node["location"], node["inputType"],node["inConstants"]);
    }
    Node* getIdentifier(json node) {

        return new Identifier(node["location"], node["value"]);
    }

    Node* getDoubleLiteral(json node)
    {
        return new DoubleLiteral(node["location"], node["value"]);
    }

    Node* getInt32Literal(json node)
    {
        return new Int32Literal(node["location"], node["value"]);
    }
    Node* getDate32Literal(json node)
    {
        return new Date32Literal(node["location"], node["value"]);
    }
    Node* getDayTimeIntervalLiteral(json node)
    {
        return new DayTimeIntervalLiteral(node["location"], node["value"]);
    }


    Node* getInt64Literal(json node)
    {
        return new Int64Literal(node["location"], node["value"]);
    }

    Node* getStringLiteral(json node)
    {
        return new StringLiteral(node["location"], node["value"]);
    }

};




class AstNodeTreeDeserializer
{
    AstNodeDeserializer nodeDes;
public:

    AstNodeTreeDeserializer(){

    }

    Node* deserializeNode(string nodeName,json nodeJson)
    {
        return nodeDes.deserialize(nodeName,nodeJson);
    }
    Node* deserializeSource(json sourceJson)
    {

        Node *node = NULL;
        vector<Node*> sourceNodes;

        for(auto elem : sourceJson.items()){

            if(elem.key().compare("childs") == 0) {
                if(!elem.value().is_array()) {

                    sourceNodes.push_back(deserializeSource(elem.value()));
                }
                else
                {

                    json jsonArray = elem.value();
                    for(int i = 0 ; i < jsonArray.size() ; i++)
                    {
                        sourceNodes.push_back(deserializeSource(jsonArray[i]));
                    }
                }
            }
            else
            { //cout << elem.key() << endl;
                node = deserializeNode(elem.key(),elem.value());
            }
        }

        if(sourceNodes.size() > 0)
            node->addChilds(sourceNodes);


        return node;

    }


    std::shared_ptr<AstNodePtr> Deserialize(string plantree)
    {
        json treeJson = nlohmann::json::parse(plantree);

        Node *root  = deserializeSource(treeJson);

        std::shared_ptr<AstNodePtr> astNodePtr = std::make_shared<AstNodePtr>(root);

        return astNodePtr;

    }

};












#endif //OLVP_NODETREEDESERIALIZATION_HPP
