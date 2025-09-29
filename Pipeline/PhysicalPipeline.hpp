//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_PHYSICALPIPELINE_HPP
#define OLVP_PHYSICALPIPELINE_HPP


#include "../Operators/Operator.hpp"
#include "../Utils/ArrowRecordBatchViewer.hpp"
#include <pthread.h>


class PhysicalPipeline
{


public:

    static  void setNameOn()
    {
        char name[100];
        pthread_getname_np(pthread_self(),name,100);
        string nameStr = string(name);
        nameStr.pop_back();
        nameStr.pop_back();
        nameStr.pop_back();
        nameStr+="ON";
        pthread_setname_np(pthread_self(),nameStr.c_str());
    }
    static  void setNameOff()
    {
        char name[100];
        pthread_getname_np(pthread_self(),name,100);
        string nameStr = string(name);
        nameStr.pop_back();
        nameStr.pop_back();
        nameStr+="OFF";
        pthread_setname_np(pthread_self(),nameStr.c_str());
    }
    static int runPipeline(std::shared_ptr<vector<std::shared_ptr<Operator>>>  physicalPipeline,shared_ptr<DriverContext> driverContext)
    {


        setNameOn();
        auto tid = gettid();
        driverContext->addTids({tid});

        vector<std::shared_ptr<Operator>> operatorVectors;
        operatorVectors = *physicalPipeline;


        bool run = false;
        bool abort = false;




        for (int i = 0 ;  i < operatorVectors.size(); i++)
        {
            if (operatorVectors[i]->isFinished() == false)
                run = true;
        }

        try {

            while (run) {



                for (int i = 0; i < operatorVectors.size() - 1; i++) {


                    if (operatorVectors[i + 1]->needsInput()) {
                        std::shared_ptr<DataPage> outputPage = operatorVectors[i]->getOutput();



                        if (outputPage != NULL) {

                            if(outputPage->isExtension()) {
                                if (!operatorVectors[i + 1]->isExtension()) {

                                    spdlog::debug("Page in GPU."+operatorVectors[i]->getOperatorType()+" download to CPU for"+ operatorVectors[i+1]->getOperatorType());
                                    outputPage = operatorVectors[i]->downloadToCPU(outputPage);
                                }
                                else
                                    ;
                            }
                            else if(operatorVectors[i+1]->isExtension()) {

                                spdlog::debug("Page in CPU. Operator in GPU."+operatorVectors[i+1]->getOperatorType()+" upload to GPU");
                                outputPage = operatorVectors[i + 1]->uploadToExtension(outputPage);
                            }
                            operatorVectors[i + 1]->addInput(outputPage);
                        }
                    }

                }

                std::shared_ptr<DataPage> finalOutput = operatorVectors[operatorVectors.size() - 1]->getOutput();
                if (finalOutput != NULL && !finalOutput->isEndPage())
                    ArrowRecordBatchViewer::PrintBatchRows(finalOutput->get());


                run = false;
                for (int i = 0; i < operatorVectors.size(); i++) {
                    if (!operatorVectors[i]->isFinished())
                        run = true;
                    if(operatorVectors[i]->isAborted())
                        abort = true;
                }

                if(abort)
                    abortRemoteSourcePipeline(operatorVectors);

            }
        }
        catch (exception &e)
        {
            string error = e.what();
            spdlog::critical("Pipeline Runtime error! "+ error);

            for(auto op : operatorVectors)
                spdlog::info(op->getOperatorType());


        }

        setNameOff();
        driverContext->removeTids({tid});

        return 1;
    }


    static void abortRemoteSourcePipeline(const vector<std::shared_ptr<Operator>>& operatorVector)
    {
        for(auto op : operatorVector)
        {
            if(op->getOperatorType() == "RemoteSourceOperator") {
                op->abort();
                break;
            }
        }
    }


};

#endif //OLVP_PHYSICALPIPELINE_HPP
