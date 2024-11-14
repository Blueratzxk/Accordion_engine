//
// Created by zxk on 5/18/23.
//

#ifndef OLVP_EXPRASTPROJECTOR_HPP
#define OLVP_EXPRASTPROJECTOR_HPP


#include "ExprAstProjectorComplier.hpp"
#include "../Frontend/AstNodes/ExprOutput.hpp"

class ExprAstProjector
{

    vector<std::shared_ptr<AstNodePtr>> Exprs;

    std::shared_ptr<ExprAstProjectorComplier> compiler;
    vector<ExprOutput> outputs;
    std::vector<std::shared_ptr<gandiva::Expression>> expressions;
    std::shared_ptr<arrow::Schema> input_schema;

    std::shared_ptr<Projector> projector = NULL;

public:
    ExprAstProjector(std::shared_ptr<arrow::Schema> input_schemaIn,vector<std::shared_ptr<AstNodePtr>> ExprAstTrees,vector<ExprOutput> outputsIn)
    {
        this->Exprs = ExprAstTrees;
        this->outputs = outputsIn;
        this->compiler = std::make_shared<ExprAstProjectorComplier>();
        this->input_schema = input_schemaIn;
    }

    ExprAstProjector(std::shared_ptr<arrow::Schema> input_schemaIn,vector<Node*> ExprAstTrees,vector<ExprOutput> outputsIn)
    {
        vector<std::shared_ptr<AstNodePtr>> nodes;
        for(auto node : ExprAstTrees)
        {
            nodes.push_back(std::make_shared<AstNodePtr>(node));
        }

        this->Exprs = nodes;
        this->outputs = outputsIn;
        this->compiler = std::make_shared<ExprAstProjectorComplier>();
        this->input_schema = input_schemaIn;
    }

    std::shared_ptr<arrow::Schema> getInputSchema()
    {
        return this->input_schema;
    }

    void Parse()
    {

        for(int i = 0 ; i < this->Exprs.size() ; i++) {



            ArrowExprNode *exprNode = (ArrowExprNode *) this->compiler->Visit(this->Exprs[i]->get(), NULL);

            std::shared_ptr<arrow::Field> outputField = arrow::field(this->outputs[i].getName(),
                                                                     Typer::getType(this->outputs[i].getType()));

            std::shared_ptr<gandiva::Expression> expression =
                    TreeExprBuilder::MakeExpression(exprNode->get(), outputField);

            expressions.push_back(expression);

            delete(exprNode);
        }
    }

    bool MakePojector()
    {

        arrow::Status status;
        status = Projector::Make(this->input_schema, this->expressions, &this->projector);

        //cout << this->input_schema->ToString()<<endl;
        if(!status.ok())
        {
            spdlog::critical(status.ToString());
        }

        return status.ok();

    }

    std::shared_ptr<arrow::RecordBatch> Project(std::shared_ptr<arrow::RecordBatch> batchToProcess,std::shared_ptr<arrow::Schema> output_schema)
    {

        auto pool = arrow::default_memory_pool();
        arrow::ArrayVector outputsArray;

        arrow::Status status;
        status = projector->Evaluate(*batchToProcess, pool, &outputsArray);

        if(!status.ok()) {
            spdlog::critical("Project ERROR! "+status.ToString());
            return NULL;
        }

        std::shared_ptr<arrow::RecordBatch> result = arrow::RecordBatch::Make(output_schema, outputsArray[0]->length(), outputsArray);
        return result;

    }



    static Status test()
    {
        Status status;


        std::shared_ptr<arrow::Schema> schemaIn = arrow::schema({arrow::field("age", arrow::float64()),arrow::field("money", arrow::float64()),arrow::field("money2", arrow::float64())});


        Column *col2 = new Column("0","money","double");
        Column *col = new Column("0","age","double");

        FunctionCall *funcName = new FunctionCall("0","divide","double");
        funcName->addChilds({col,col2});

        DoubleLiteral *ageValue2 = new DoubleLiteral("0","12333");
        FunctionCall *funcName2 = new FunctionCall("0","multiply","double");
        funcName2->addChilds({funcName,ageValue2});

        ExprOutput output("age1","double");
        ExprOutput output2("age2","double");



        vector<Node*> nodes = {funcName2,funcName2};
        ExprAstProjector projector1(schemaIn,nodes,{output,output2});

        projector1.Parse();
        if(projector1.MakePojector())
        {

            arrow::DoubleBuilder builder;
            double_t values[4] = {12133, 2123,2323, 43244};
            ARROW_RETURN_NOT_OK(builder.AppendValues(values, 4));
            ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array, builder.Finish());
            auto in_batch = arrow::RecordBatch::Make(schemaIn, 4, {array,array,array});


            std::shared_ptr<arrow::Schema> output_schema = arrow::schema({arrow::field("age1", arrow::float64()),arrow::field("age2", arrow::float64())});
            std::shared_ptr<arrow::RecordBatch> re = projector1.Project(in_batch,output_schema);

            if(re != NULL) {
                cout << re->ToString() << endl;
            }
            else
            {
                cout << "ERROR!" << endl;
            }
        }


        return status;


    }




};



#endif //OLVP_EXPRASTPROJECTOR_HPP
