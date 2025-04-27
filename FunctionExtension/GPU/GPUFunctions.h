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

};





#endif //OLVP_GPUFUNCTIONS_H
