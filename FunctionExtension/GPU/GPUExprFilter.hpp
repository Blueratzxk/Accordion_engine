//
// Created by zxk on 8/16/25.
//

#ifndef OLVP_GPUEXPRFILTER_HPP
#define OLVP_GPUEXPRFILTER_HPP


#include "GPUFilterExprEvaluator.hpp"
class GPUExprFilter
{
    std::shared_ptr<AstNodePtr> Expr;
    std::shared_ptr<arrow::Schema> input_schema;
    GPUFunctions gpuFunctions;

public:
    GPUExprFilter(std::shared_ptr<arrow::Schema> input_schemaIn,std::shared_ptr<AstNodePtr> ExprAstTree)
    {
        this->Expr = ExprAstTree;
        this->input_schema = input_schemaIn;

    }
    GPUExprFilter()
    {
    }

    shared_ptr<DataPage> evaluate(shared_ptr<DataPage> page)
    {
        shared_ptr<AstTreeEvaluator> astTreeEvaluator = make_shared<AstTreeEvaluator>(this->input_schema,page);
        auto re = astTreeEvaluator->Visit(Expr->get(),NULL);
        auto *result = (EvaResult*)re;
        int elementCount = 0;
        auto final = astTreeEvaluator->filter(result->getColumn(),elementCount);

        result->freeResult();
        delete result;
        return make_shared<DataPage>(final,this->input_schema,elementCount,DataPage::GPU);
    }


    void test()
    {
        vector<FieldDesc> fieldsIn = {FieldDesc("l_orderkey","int64"),
                                      FieldDesc("l_partkey","int64"),
                                      FieldDesc("l_suppkey","int64"),
                                      FieldDesc("l_linenumber","int64"),
                                      FieldDesc("l_quantity","int64"),
                                      FieldDesc("l_extendedprice","double"),
                                      FieldDesc("l_discount","double"),
                                      FieldDesc("l_tax","double"),
                                      FieldDesc("l_returnflag","string"),
                                      FieldDesc("l_linestatus","string"),
                                      FieldDesc("l_shipdate","date32"),
                                      FieldDesc("l_commitdate","date32"),
                                      FieldDesc("l_receiptdate","date32"),
                                      FieldDesc("l_shipinstruct","string"),
                                      FieldDesc("l_shipmode","string"),
                                      FieldDesc("l_comment","string")};

        arrow::FieldVector vector;

        for(auto v : fieldsIn)
        {
            vector.push_back(make_shared<arrow::Field>(v.getFieldName(),Typer::getType(v.getFieldType())));
        }
        this->input_schema = make_shared<arrow::Schema>(vector);

        arrow::Int64Builder l_orderkey;
        l_orderkey.Append(1).ok();
        l_orderkey.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array;
        l_orderkey.Finish(&id_array).ok();

        arrow::Int64Builder l_partkey;
        l_partkey.Append(1).ok();
        l_partkey.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array2;
        l_partkey.Finish(&id_array2).ok();

        arrow::Int64Builder l_suppkey;
        l_suppkey.Append(1).ok();
        l_suppkey.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array3;
        l_suppkey.Finish(&id_array3).ok();

        arrow::Int64Builder l_linenumber;
        l_linenumber.Append(1).ok();
        l_linenumber.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array4;
        l_linenumber.Finish(&id_array4).ok();

        arrow::Int64Builder l_quantity;
        l_quantity.Append(1).ok();
        l_quantity.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array5;
        l_quantity.Finish(&id_array5).ok();


        arrow::DoubleBuilder l_extendedprice;
        l_extendedprice.Append(1).ok();
        l_extendedprice.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array6;
        l_extendedprice.Finish(&id_array6).ok();


        arrow::DoubleBuilder l_discount;
        l_discount.Append(1).ok();
        l_discount.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array7;
        l_discount.Finish(&id_array7).ok();

        arrow::DoubleBuilder l_tax;
        l_tax.Append(1).ok();
        l_tax.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array8;
        l_tax.Finish(&id_array8).ok();


        arrow::StringBuilder l_returnflag;
        l_returnflag.Append("A").ok();
        l_returnflag.Append("B").ok();
        std::shared_ptr<arrow::Array> id_array9;
        l_returnflag.Finish(&id_array9).ok();

        arrow::StringBuilder l_linestatus;
        l_linestatus.Append("A").ok();
        l_linestatus.Append("B").ok();
        std::shared_ptr<arrow::Array> id_array10;
        l_linestatus.Finish(&id_array10).ok();


        arrow::Date32Builder l_shipdate;
        l_shipdate.Append(1).ok();
        l_shipdate.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array11;
        l_shipdate.Finish(&id_array11).ok();

        arrow::Date32Builder l_commitdate;
        l_commitdate.Append(1).ok();
        l_commitdate.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array12;
        l_commitdate.Finish(&id_array12).ok();

        arrow::Date32Builder l_receiptdate;
        l_receiptdate.Append(1).ok();
        l_receiptdate.Append(2).ok();
        std::shared_ptr<arrow::Array> id_array13;
        l_receiptdate.Finish(&id_array13).ok();

        arrow::StringBuilder l_shipinstruct;
        l_shipinstruct.Append("11").ok();
        l_shipinstruct.Append("11").ok();
        std::shared_ptr<arrow::Array> id_array14;
        l_shipinstruct.Finish(&id_array14).ok();

        arrow::StringBuilder l_shipmode;
        l_shipmode.Append("11").ok();
        l_shipmode.Append("11").ok();
        std::shared_ptr<arrow::Array> id_array15;
        l_shipmode.Finish(&id_array15).ok();

        arrow::StringBuilder l_comment;
        l_comment.Append("11").ok();
        l_comment.Append("11").ok();
        std::shared_ptr<arrow::Array> id_array16;
        l_comment.Finish(&id_array16).ok();

        auto record_batch = arrow::RecordBatch::Make(
                input_schema,
                2,  // ÐÐÊý
                {id_array, id_array2, id_array3, id_array4, id_array5, id_array6, id_array7, id_array8,
                 id_array9, id_array10, id_array11, id_array12, id_array13,
                 id_array14, id_array15, id_array16 }
        );


        Date32Literal *date = new Date32Literal("0","1998-12-01");

        DayTimeIntervalLiteral *interval = new DayTimeIntervalLiteral("0",90);
        FunctionCall *funcDatetimeCompute = new FunctionCall("0","subtract","int32");
        funcDatetimeCompute->addChilds({date,interval});

        FunctionCall *castDate = new FunctionCall("0","castDATE","date32");
        castDate->addChilds({funcDatetimeCompute});

        Column *lshipDate = new Column("0","l_shipdate","date32");

        FunctionCall *funcLess = new FunctionCall("0","less_than_or_equal_to","bool");
        funcLess->addChilds({lshipDate,castDate});


        this->Expr = make_shared<AstNodePtr>(funcLess);

        auto re = gpuFunctions.pushCPUPageToGPU(record_batch);

        this->evaluate(make_shared<DataPage>(re,input_schema,2,DataPage::GPU));
    }


};


#endif //OLVP_GPUEXPRFILTER_HPP
