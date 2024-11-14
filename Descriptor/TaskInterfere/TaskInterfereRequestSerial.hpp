//
// Created by zxk on 6/9/23.
//

#ifndef OLVP_TASKINTERFEREREQUESTSERIAL_HPP
#define OLVP_TASKINTERFEREREQUESTSERIAL_HPP

#include "TaskIntraParaUpdateRequest.hpp"
#include "TaskInterfereRequest.hpp"
#include "spdlog/spdlog.h"
#include "TaskBufferOperatingRequest.hpp"

class TaskInterfereSerializer {

public:
    TaskInterfereSerializer() {

    }

    static string Serialize(shared_ptr<TaskInterfereRequest> request) {
        nlohmann::json json;

        if (request->getType() == "TaskIntraParaUpdateRequest") {
            json["taskInterfereType"] = "TaskIntraParaUpdateRequest";
            json["request"] = TaskIntraParaUpdateRequest::Serialize(
                    *static_pointer_cast<TaskIntraParaUpdateRequest>(request));

            string result = json.dump();
            return result;
        }
        else if(request->getType() == "TaskBufferOperatingRequest")
        {
            json["taskInterfereType"] = "TaskBufferOperatingRequest";
            json["request"] = TaskBufferOperatingRequest::Serialize(
                    *static_pointer_cast<TaskBufferOperatingRequest>(request));

            string result = json.dump();
            return result;
        }
        else
        {
            spdlog::critical("Unknown TaskInterfereRequest Type!");
            return "NULL";
        }

    }


    static shared_ptr<TaskInterfereRequest> Deserialize(string request) {

        if(request == "NULL")
            return NULL;


        nlohmann::json json = nlohmann::json::parse(request);

        shared_ptr<TaskInterfereRequest> result;
        string type = json["taskInterfereType"];

        if (type == "TaskIntraParaUpdateRequest") {
            result = TaskIntraParaUpdateRequest::Deserialize(json["request"]);
        }
        else if (type == "TaskBufferOperatingRequest") {
            result = TaskBufferOperatingRequest::Deserialize(json["request"]);
        }
        else
        {
            spdlog::critical("Unknown TaskInterfereRequest Type!");
            return NULL;
        }


        return result;
    }



};


#endif //OLVP_TASKINTERFEREREQUESTSERIAL_HPP
