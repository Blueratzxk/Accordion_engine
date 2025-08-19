//
// Created by zxk on 3/4/25.
//

#ifndef OLVP_GPUFUNCTIONS_H
#define OLVP_GPUFUNCTIONS_H


#include <dlfcn.h>
#include "../FuncExt.h"
#include "string"
#include "spdlog/spdlog.h"
#include "arrow/api.h"
using namespace std;


typedef bool (*extendUsable)();

typedef void (*inner_join_GPU)(shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,
                    vector<int> probeHashChannels,vector<int> buildHashChannels,vector<int> probeOutputChannels,vector<int> buildOutputChannels,
                    void* cudfProbe,void* cudfBuild, shared_ptr<arrow::Schema> &outputSchema,void* &outputGPUTable,int &elementCount);

typedef void (*viewPageGPU)(void* GPUPage);
typedef void* (*pushPageToGPU)(shared_ptr<arrow::RecordBatch> input);
typedef shared_ptr<arrow::Table> (*getPageFromGPU)(void* GPUPage,shared_ptr<arrow::Schema> outputSchema);


typedef void (*maintainBuildTableInGPUByToken)(string token,shared_ptr<arrow::Table> buildTable);
typedef string (*getNextToken)();

typedef int (*deleteBuildTableInGPUByToken)(string token);
typedef bool (*isBuildTableExistByToken)(string token);


typedef void (*inner_join_GPU_By_Token)(shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,
                             vector<int> probeHashChannels,vector<int> buildHashChannels,vector<int> probeOutputChannels,vector<int> buildOutputChannels,
                             void* cudfProbe,string buildTableToken, shared_ptr<arrow::Schema> &outputSchema,void* &outputGPUTable,int &elementCount);

typedef void (*inner_join_GPU_ByToken)(shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,shared_ptr<arrow::Schema> buildOutputSchema,
                            vector<int> probeHashChannels,vector<int> buildHashChannels,vector<int> probeOutputChannels,vector<int> buildOutputChannels,
                            void* cudfProbe,string buildTableToken,void* &outputGPUTable,int &elementCount);

typedef void (*releaseGPUPage)(void *table);
typedef void (*releaseGPUColumn)(void *column);


typedef void* (*cudf_functionCall)(string funcName,vector<void*> args,string outputType);
typedef void* (*cudf_get_column_by_index)(void *cudfTable,int index);
typedef void* (*cudf_make_column_from_int32_scalar)(int value, int num_rows);
typedef void* (*cudf_make_column_from_int64_scalar)(int64_t value, int num_rows);
typedef void* (*cudf_make_column_from_double_scalar)(double value, int num_rows);
typedef void* (*cudf_make_column_from_string_scalar)(string value, int num_rows);
typedef void* (*cudf_make_column_from_date32_scalar)(string value, int num_rows);
typedef void* (*cudf_apply_boolean_mask)(void *cudfTable,void* finalmask, int &elementCount);
typedef void* (*cudf_make_table)(vector<void*> columns);
typedef void* (*cudf_copy_column_by_index)(void* table_ptr, int index);
typedef void* (*cudf_aggregation)(void* cudf_table,vector<int> group_columns,vector<int> agg_columns,
        vector<string> agg_types,vector<string> &outputTypes,int &elementCount);


class GPUFunctions:public FuncExt
{
    string libPath = "Glibs/GPU_LIBS";
    void *handle = NULL;
public:
    GPUFunctions(){}


    bool load()
    {
        handle = dlopen(libPath.c_str(), RTLD_NOW);
        if(!handle) {
            //spdlog::debug("No GPU extension. Cannot find GPU_LIBS.so.");
            //fprintf(stderr, "%s\n", dlerror());
            return false;
        }
        return  true;
    }
    void extensionUsableCheck() override
    {
      //  if(extensionUsable())
        //    spdlog::info("GPU extension usable!");
    }
    bool extensionUsable() override
    {
        if(!load())
            return false;

        extendUsable p = (extendUsable)dlsym(handle, "extendUsable");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load extendUsable failed");
            return false;
        }
        return p();
    }

    ~GPUFunctions()
    {
        if(handle != NULL)
            dlclose(handle);
    }




    void inner_join(shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,
                                vector<int> probeHashChannels,vector<int> buildHashChannels,vector<int> probeOutputChannels,vector<int> buildOutputChannels,
                                void* cudfProbe,void* cudfBuild, shared_ptr<arrow::Schema> &outputSchema,void* &outputGPUTable,int &elementCount)
    {



        if(!load())
            return;

        inner_join_GPU p = (inner_join_GPU)dlsym(handle, "inner_join_GPU");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load inner_join_GPU failed");
            return;
        }

        return p(probeInputSchema,buildInputSchema,probeHashChannels,buildHashChannels,probeOutputChannels,buildOutputChannels,
                cudfProbe,cudfBuild, outputSchema,outputGPUTable,elementCount);


    }
    void inner_join_by_token(shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,
                    vector<int> probeHashChannels,vector<int> buildHashChannels,vector<int> probeOutputChannels,vector<int> buildOutputChannels,
                    void* cudfProbe,string buildToken, shared_ptr<arrow::Schema> &outputSchema,void* &outputGPUTable,int &elementCount)
    {

        if(!load())
            return;

        inner_join_GPU_By_Token p = (inner_join_GPU_By_Token)dlsym(handle, "inner_join_GPU_By_Token");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load inner_join_GPU_By_Token failed");
            return;
        }

        return p(probeInputSchema,buildInputSchema,probeHashChannels,buildHashChannels,probeOutputChannels,buildOutputChannels,
                 cudfProbe,buildToken, outputSchema,outputGPUTable,elementCount);
    }

    void inner_join_byToken(shared_ptr<arrow::Schema>probeInputSchema,shared_ptr<arrow::Schema>buildInputSchema,shared_ptr<arrow::Schema>buildOutputSchema,
                             vector<int> probeHashChannels,vector<int> buildHashChannels,vector<int> probeOutputChannels,vector<int> buildOutputChannels,
                             void* cudfProbe,string buildToken,void* &outputGPUTable,int &elementCount)
    {

        if(!load())
            return;

        inner_join_GPU_ByToken p = (inner_join_GPU_ByToken)dlsym(handle, "inner_join_GPU_ByToken");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load inner_join_GPU_ByToken failed");
            return;
        }

        return p(probeInputSchema,buildInputSchema,buildOutputSchema,probeHashChannels,buildHashChannels,probeOutputChannels,buildOutputChannels,
                 cudfProbe,buildToken,outputGPUTable,elementCount);
    }

    void ViewPage(void* GPUPage){

        if(!load())
            return;

        viewPageGPU p = (viewPageGPU)dlsym(handle, "viewPageGPU");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load viewPageGPU failed");
            return;
        }

        return p(GPUPage);
    }

    void* pushCPUPageToGPU(shared_ptr<arrow::RecordBatch> input){

        if(!load())
            return NULL;

        pushPageToGPU p = (pushPageToGPU)dlsym(handle, "pushPageToGPU");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load pushPageToGPU failed");
            return NULL;
        }

        return p(input);

    }
    shared_ptr<arrow::Table> getCPUPageFromGPU(void* GPUPage,shared_ptr<arrow::Schema> outputSchema){

        if(!load())
            return NULL;

        getPageFromGPU p = (getPageFromGPU)dlsym(handle, "getPageFromGPU");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load getPageFromGPU failed");
            return NULL;
        }

        return p(GPUPage,outputSchema);

    }


    void maintainBuildTableByToken(string token,shared_ptr<arrow::Table> buildTable){

        if(!load())
            return;

        maintainBuildTableInGPUByToken p = (maintainBuildTableInGPUByToken)dlsym(handle, "maintainBuildTableInGPUByToken");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load maintainBuildTableInGPUByToken failed");
            return;
        }

        return p(token,buildTable);
    }
    int deleteBuildTableByToken(string token){

        if(!load())
            return -1;

        deleteBuildTableInGPUByToken p = (deleteBuildTableInGPUByToken)dlsym(handle, "deleteBuildTableInGPUByToken");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load deleteBuildTableInGPUByToken failed");
            return false;
        }

        return p(token);

    }
    bool isBuildTableExist(string token){

        if(!load())
            return false;

        isBuildTableExistByToken p = (isBuildTableExistByToken)dlsym(handle, "isBuildTableExistByToken");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load isBuildTableExistByToken failed");
            return false;
        }

        return p(token);
    }

    string getNextTokenStr(){

        if(!load())
            return "0";

        getNextToken p = (getNextToken)dlsym(handle, "getNextToken");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load getNextToken failed");
            return "0";
        }

        return p();
    }




    void *cudfFunctionCall(string funcName,vector<void*> args,string outputType){

        if(!load())
            return NULL;

        cudf_functionCall p = (cudf_functionCall)dlsym(handle, "cudf_functionCall");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_functionCall failed");
            return NULL;
        }

        return p(funcName,args,outputType);

    }
    void *cudfGetColumnByIndex(void *cudfTable,int index){

        if(!load())
            return NULL;

        cudf_get_column_by_index p = (cudf_get_column_by_index)dlsym(handle, "cudf_get_column_by_index");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_get_column_by_index failed");
            return NULL;
        }

        return p(cudfTable,index);
    }
    void *cudfMakeColumnFromInt32Scalar(int value, int num_rows){

        if(!load())
            return NULL;

        cudf_make_column_from_int32_scalar p = (cudf_make_column_from_int32_scalar)dlsym(handle, "cudf_make_column_from_int32_scalar");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_make_column_from_int32_scalar failed");
            return NULL;
        }

        return p(value,num_rows);

    }
    void *cudfMakeColumnFromInt64Scalar(int64_t value, int num_rows){

        if(!load())
            return NULL;

        cudf_make_column_from_int64_scalar p = (cudf_make_column_from_int64_scalar)dlsym(handle, "cudf_make_column_from_int64_scalar");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_make_column_from_int64_scalar failed");
            return NULL;
        }

        return p(value,num_rows);

    }
    void *cudfMakeColumnFromDoubleScalar(double value, int num_rows){

        if(!load())
            return NULL;

        cudf_make_column_from_double_scalar p = (cudf_make_column_from_double_scalar)dlsym(handle, "cudf_make_column_from_double_scalar");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_make_column_from_double_scalar failed");
            return NULL;
        }

        return p(value,num_rows);

    }
    void *cudfMakeColumnFromStringScalar(string value, int num_rows){

        if(!load())
            return NULL;

        cudf_make_column_from_string_scalar p = (cudf_make_column_from_string_scalar)dlsym(handle, "cudf_make_column_from_string_scalar");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_make_column_from_string_scalar failed");
            return NULL;
        }

        return p(value,num_rows);

    }
    void *cudfMakeColumnFromDate32Scalar(string value, int num_rows){

        if(!load())
            return NULL;

        cudf_make_column_from_date32_scalar p = (cudf_make_column_from_date32_scalar)dlsym(handle, "cudf_make_column_from_date32_scalar");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_make_column_from_date32_scalar failed");
            return NULL;
        }

        return p(value,num_rows);

    }
    void *cudfApplyBooleanMask(void* cudf_table, void* finalmask, int &elementCount){

        if(!load())
            return NULL;

        cudf_apply_boolean_mask p = (cudf_apply_boolean_mask)dlsym(handle, "cudf_apply_boolean_mask");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_apply_boolean_mask failed");
            return NULL;
        }

        return p(cudf_table,finalmask,elementCount);

    }
    void freeGPUPage(void* cudf_table){

        if(!load())
            return ;

        releaseGPUPage p = (releaseGPUPage)dlsym(handle, "releaseGPUPage");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load releaseGPUPage failed");
            return ;
        }

        return p(cudf_table);
    }

    void freeGPUColumn(void* cudf_column){

        if(!load())
            return ;

        releaseGPUColumn p = (releaseGPUColumn)dlsym(handle, "releaseGPUColumn");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load releaseGPUColumn failed");
            return ;
        }

        return p(cudf_column);
    }

    void* cudfMakeTable(vector<void*> columns){

        if(!load())
            return NULL;

        cudf_make_table p = (cudf_make_table)dlsym(handle, "cudf_make_table");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_make_table failed");
            return NULL;
        }

        return p(columns);
    }

    void* cudfCopyColumnByIndex(void* table_ptr, int index){

        if(!load())
            return NULL;

        cudf_copy_column_by_index p = (cudf_copy_column_by_index)dlsym(handle, "cudf_copy_column_by_index");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_copy_column_by_index failed");
            return NULL;
        }

        return p(table_ptr,index);
    }

    void* cudfAggregation(void* cudf_table,vector<int> group_columns,vector<int> agg_columns,
                          vector<string> agg_types,vector<string> &outputTypes, int &elementCount){

        if(!load())
            return NULL;

        cudf_aggregation p = (cudf_aggregation)dlsym(handle, "cudf_aggregation");  //argv[2]对应输入需获取地址的符号名
        if(!p) {
            spdlog::debug("Load cudf_aggregation failed");
            return NULL;
        }

        return p(cudf_table,group_columns,agg_columns,agg_types,outputTypes,elementCount);
    }


};





#endif //OLVP_GPUFUNCTIONS_H
