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

                        if (outputPage != NULL)
                            operatorVectors[i + 1]->addInput(outputPage);
                    }

                }

                std::shared_ptr<DataPage> finalOutput = operatorVectors[operatorVectors.size() - 1]->getOutput();
                if (finalOutput != NULL && !finalOutput->isEndPage())
                    ArrowRecordBatchViewer::PrintBatchRows(finalOutput->get());


                run = false;
                for (int i = 0; i < operatorVectors.size(); i++)
                    if (operatorVectors[i]->isFinished() == false)
                        run = true;

            }
        }
        catch (exception &e)
        {
            string error = e.what();
            spdlog::critical("Pipeline Runtime error! "+ error);
        }

        setNameOff();
        driverContext->removeTids({tid});

        return 1;
    }


};

#endif //OLVP_PHYSICALPIPELINE_HPP
