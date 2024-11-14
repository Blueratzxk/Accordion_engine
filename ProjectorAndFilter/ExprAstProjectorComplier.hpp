//
// Created by zxk on 5/18/23.
//

#ifndef OLVP_EXPRASTPROJECTORCOMPLIER_HPP
#define OLVP_EXPRASTPROJECTORCOMPLIER_HPP

#include "../Frontend/AstNodes/tree.h"
#include "../Frontend/AstNodes/AstNodeVisitor.h"
#include "../Frontend/AstNodes/DefaultAstExpressionVisitor.hpp"


#include "arrow/compute/api_vector.h"

#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/selection_vector.h"
#include "gandiva/tree_expr_builder.h"

#include "../Utils/ArrowDicts.hpp"
#include "ArrowExprNode.hpp"
#include "../Utils/TimeCommon.hpp"

using arrow::Datum;
using arrow::Status;
using arrow::compute::TakeOptions;
using gandiva::Condition;
using gandiva::ConfigurationBuilder;

using gandiva::Filter;

using gandiva::Projector;
using gandiva::SelectionVector;
using gandiva::TreeExprBuilder;


class ExprAstProjectorComplier: public DefaultAstExpressionVisitor
{

public:


    void* VisitFunctionCall(FunctionCall* node,void* context) {
        vector<Node*> arguments = node->getChildren();


        vector<std::shared_ptr<gandiva::Node>> functionArguments;
        for(int i = 0 ; i < arguments.size() ; i++)
        {
            ArrowExprNode *arrowExprNode = (ArrowExprNode*)Visit(arguments[i],NULL);
            functionArguments.push_back(arrowExprNode->get());
            delete arrowExprNode;
        }

        std::shared_ptr<gandiva::Node> functionNode =
                TreeExprBuilder::MakeFunction(node->getFuncName(), functionArguments, Typer::getType(node->getOutputType()));


    //    cout << node->getFuncName() << endl;

        return new ArrowExprNode(functionNode);

    }

    void* VisitDoubleLiteral(DoubleLiteral* node,void* context) {



        std::shared_ptr<gandiva::Node> literal_Double = TreeExprBuilder::MakeLiteral(node->getValue());

        return new ArrowExprNode(literal_Double);

    }
    void* VisitInt32Literal(Int32Literal* node,void* context) {

        std::shared_ptr<gandiva::Node> literal_Int32 = TreeExprBuilder::MakeLiteral(node->getValue());

        return new ArrowExprNode(literal_Int32);
    }
    void* VisitInt64Literal(Int64Literal* node,void* context) {


        std::shared_ptr<gandiva::Node> literal_Int64 = TreeExprBuilder::MakeLiteral(node->getValue());

        return new ArrowExprNode(literal_Int64);
    }
    void* VisitStringLiteral(StringLiteral* node,void* context){

        std::shared_ptr<gandiva::Node> literal_Int32 = TreeExprBuilder::MakeStringLiteral(node->getValue());

        return new ArrowExprNode(literal_Int32);
    }
    void* VisitDate32Literal(Date32Literal* node,void* context){

        int32_t date;
        TimeCommon::getDate32(node->getValue(),&date);
        std::shared_ptr<gandiva::Node> literal_Date32 = TreeExprBuilder::MakeLiteral(date);

        return new ArrowExprNode(literal_Date32);
    }

    void* VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral* node,void* context){

        std::shared_ptr<gandiva::Node> DayTimeIntervalLiteral = TreeExprBuilder::MakeLiteral(node->getValue());
        return new ArrowExprNode(DayTimeIntervalLiteral);

    }

    void *VisitIdentifier(Identifier* node,void* context)
    {
        return new ArrowExprNode(NULL);
    }
    void *VisitColumn(Column* node,void* context)
    {

        std::shared_ptr<arrow::Field> field = arrow::field(node->getValue(),Typer::getType(node->getColumnType()));
        std::shared_ptr<gandiva::Node> fieldNode = TreeExprBuilder::MakeField(field);
        return new ArrowExprNode(fieldNode);
    }

    void *VisitIfExpression(IfExpression *node,void *context)
    {
        vector<Node*> arguments = node->getChildren();


        vector<std::shared_ptr<gandiva::Node>> IfExpressionBody;
        for(int i = 0 ; i < arguments.size() ; i++)
        {
            ArrowExprNode *arrowExprNode = (ArrowExprNode*)Visit(arguments[i],NULL);
            IfExpressionBody.push_back(arrowExprNode->get());
            delete arrowExprNode;
        }

        auto ifNode = TreeExprBuilder::MakeIf(IfExpressionBody[0], IfExpressionBody[1], IfExpressionBody[2],Typer::getType(node->getOutputType()));
        return new ArrowExprNode(ifNode);

    }



    void *VisitInExpression(InExpression *node,void *context)
    {
        vector<Node*> arguments = node->getChildren();


        vector<std::shared_ptr<gandiva::Node>> InExpressionCondition;
        for(int i = 0 ; i < arguments.size() ; i++)
        {
            ArrowExprNode *arrowExprNode = (ArrowExprNode*)Visit(arguments[i],NULL);
            InExpressionCondition.push_back(arrowExprNode->get());
            delete arrowExprNode;
        }


        auto inNode = getInExpressionByType(InExpressionCondition[0],node->getInputType(),node->getInConstants());
        return new ArrowExprNode(inNode);

    }

    shared_ptr<gandiva::Node> getInExpressionByType(std::shared_ptr<gandiva::Node> node,string type,vector<string> values)
    {
        shared_ptr<gandiva::Node> in_expr;

        if(type == "int64"){
            std::unordered_set<int64_t> in_constants;
            for(auto value : values)
                in_constants.insert(stol(value.c_str()));
            in_expr = TreeExprBuilder::MakeInExpressionInt64(node,in_constants);
        }
        else if(type == "int32"){
            std::unordered_set<int32_t> in_constants;
            for(auto value : values)
                in_constants.insert(stoi(value.c_str()));
            in_expr = TreeExprBuilder::MakeInExpressionInt32(node,in_constants);
        }
        else if(type == "double"){
            std::unordered_set<double> in_constants;
            for(auto value : values)
                in_constants.insert(stod(value.c_str()));
            in_expr = TreeExprBuilder::MakeInExpressionDouble(node,in_constants);
        }
        else if(type == "float"){
            std::unordered_set<float> in_constants;
            for(auto value : values)
                in_constants.insert(stof(value.c_str()));
            in_expr = TreeExprBuilder::MakeInExpressionFloat(node,in_constants);
        }
        else if(type == "string"){
            std::unordered_set<string> in_constants;
            for(auto value : values)
                in_constants.insert(value);
            in_expr = TreeExprBuilder::MakeInExpressionString(node,in_constants);
        }
        else if(type == "date32"){
            std::unordered_set<int32_t> in_constants;
            for(auto value : values)
                in_constants.insert(stoi(value.c_str()));
            in_expr = TreeExprBuilder::MakeInExpressionDate32(node,in_constants);
        }
        else
        {
            spdlog::critical("Cannot find this InExpression input type!");
            return NULL;
        }

        return in_expr;

    }





};



#endif //OLVP_EXPRASTPROJECTORCOMPLIER_HPP
