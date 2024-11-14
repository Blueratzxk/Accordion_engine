//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_EXPRASTFILTER_HPP
#define OLVP_EXPRASTFILTER_HPP

#include "ExprAstFilterComplier.hpp"
#include "../Frontend/AstNodes/ExprOutput.hpp"

class ExprAstFilter
{

    std::shared_ptr<AstNodePtr> Expr;

    std::shared_ptr<ExprAstFilterComplier> compiler;
    vector<ExprOutput> outputs;
    std::shared_ptr<Condition> condition;
    std::shared_ptr<arrow::Schema> input_schema;

    std::shared_ptr<gandiva::Filter> filter = NULL;

public:
    ExprAstFilter(std::shared_ptr<arrow::Schema> input_schemaIn,Node* ExprAstTree)
    {
        this->Expr = std::make_shared<AstNodePtr>(ExprAstTree);
        this->compiler = std::make_shared<ExprAstFilterComplier>();
        this->input_schema = input_schemaIn;
    }

    ExprAstFilter(std::shared_ptr<arrow::Schema> input_schemaIn,std::shared_ptr<AstNodePtr> ExprAstTree)
    {
        this->Expr = ExprAstTree;
        this->compiler = std::make_shared<ExprAstFilterComplier>();
        this->input_schema = input_schemaIn;
    }

    void Parse() {

        ArrowExprNode *exprNode = (ArrowExprNode *) this->compiler->Visit(this->Expr->get(), NULL);

        this->condition = TreeExprBuilder::MakeCondition(exprNode->get());

    }

    arrow::Status MakeFilter()
    {

        arrow::Status status;
        status = gandiva::Filter::Make(this->input_schema, this->condition, &this->filter);
        return status;

    }

    std::shared_ptr<arrow::RecordBatch> DoFilter(std::shared_ptr<arrow::RecordBatch> batchToProcess)
    {

        auto pool = arrow::default_memory_pool();

        arrow::Status status;

        std::shared_ptr<gandiva::SelectionVector> result_indices;
        // Use 16-bit integers for indices. Result can be no longer than input size,
        // so use batch num_rows as max_slots.
        status = gandiva::SelectionVector::MakeInt32(/*max_slots=*/batchToProcess->num_rows(), pool,
                                                                   &result_indices);

        if(!status.ok()) {
            spdlog::critical("Filter result_indices created failed!");
            return NULL;
        }

        status = filter->Evaluate(*batchToProcess, result_indices);
        if(!status.ok()) {
            cout << input_schema->ToString()<<endl<<"----------------------------"<<endl;
            cout << batchToProcess->schema()->ToString()<<endl;
            spdlog::critical("Filter evaluating failed!"+status.ToString());
            return NULL;
        }

        std::shared_ptr<arrow::Array> take_indices = result_indices->ToArray();
        Datum maybe_batch;

        arrow::Result<arrow::Datum> resultDatum = arrow::compute::Take(Datum(batchToProcess), Datum(take_indices),
                                                   TakeOptions::NoBoundsCheck());

        if(resultDatum.ok())
        {
            maybe_batch = resultDatum.ValueOrDie();
        }
        else
        {
            spdlog::critical("resultDatum creating failed!");
            return NULL;
        }


        std::shared_ptr<arrow::RecordBatch> result = maybe_batch.record_batch();
        //(Doc section: Evaluate filter)

        if(result == NULL)
        {
            spdlog::critical("filter result ERROR!");
            return NULL;
        }

        return result;

    }



    static Status test()
    {
        Status status;


        std::shared_ptr<arrow::Schema> schemaIn = arrow::schema({arrow::field("age", arrow::float64()),arrow::field("money", arrow::float64()),arrow::field("money2", arrow::float64())});


        Column *col2 = new Column("0","money","double");
        Column *col = new Column("0","age","double");

        FunctionCall *funcEqual = new FunctionCall("0","equal","bool");
        funcEqual->addChilds({col,col2});

        DoubleLiteral *ageValue2 = new DoubleLiteral("0","2125");
        DoubleLiteral *valueMul = new DoubleLiteral("0","2");
        FunctionCall *funcMultiply = new FunctionCall("0","multiply","double");
        funcMultiply->addChilds({ageValue2,valueMul});

        FunctionCall *funcLess = new FunctionCall("0","greater_than","bool");
        funcLess->addChilds({col,funcMultiply});


        FunctionCall *funcAnd = new FunctionCall("0","and","bool");
        funcAnd->addChilds({funcEqual,funcLess});


        ExprAstFilter filter1(schemaIn,funcAnd);

        filter1.Parse();
        if(filter1.MakeFilter().ok())
        {

            arrow::DoubleBuilder builder;
            double_t values[4] = {12133, 2123,2123, 43244};
            ARROW_RETURN_NOT_OK(builder.AppendValues(values, 4));
            ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array, builder.Finish());
            auto in_batch = arrow::RecordBatch::Make(schemaIn, 4, {array,array,array});



            std::shared_ptr<arrow::RecordBatch> re = filter1.DoFilter(in_batch);

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



#endif //OLVP_EXPRASTFILTER_HPP
