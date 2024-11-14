//
// Created by zxk on 6/3/23.
//

#ifndef OLVP_TASKUPDATEREQUEST_HPP
#define OLVP_TASKUPDATEREQUEST_HPP


#include "../Execution/Task/TaskSource.hpp"
#include "../Execution/Buffer/OutputBufferSchema.hpp"
#include "../Planner/Fragment.hpp"
//#include "../Descriptor/TaskInterfere/TaskInterfereRequest.hpp"
#include "../Descriptor/TaskInterfere/TaskInterfereRequestSerial.hpp"
#include "../Session/SessionRepresentation.hpp"

class TaskUpdateRequest
{
    shared_ptr<TaskSource> taskSource = NULL;
    shared_ptr<OutputBufferSchema> schema = NULL;
    shared_ptr<PlanFragment> fragment = NULL;
    shared_ptr<TaskInterfereRequest> taskInterfereRequest = NULL;
    shared_ptr<SessionRepresentation> sessionRepresentation = NULL;

public:
    TaskUpdateRequest( shared_ptr<TaskSource> taskSource,shared_ptr<OutputBufferSchema> schema,shared_ptr<PlanFragment> fragment,
                       shared_ptr<TaskInterfereRequest> taskInterfereRequest,shared_ptr<SessionRepresentation> sessionRepresentation){

        this->taskSource = taskSource;
        this->schema = schema;
        this->fragment = fragment;
        this->taskInterfereRequest = taskInterfereRequest;
        this->sessionRepresentation = sessionRepresentation;
    }

    TaskUpdateRequest( shared_ptr<TaskSource> taskSource,shared_ptr<PlanFragment> fragment){

        this->taskSource = taskSource;
        this->fragment = fragment;

    }

    TaskUpdateRequest( shared_ptr<TaskSource> taskSource,shared_ptr<OutputBufferSchema> schema){

        this->taskSource = taskSource;
        this->schema = schema;
        this->fragment = NULL;
    }

    TaskUpdateRequest(shared_ptr<OutputBufferSchema> schema){

        this->taskSource = NULL;
        this->schema = schema;
        this->fragment = NULL;
    }

    TaskUpdateRequest(shared_ptr<TaskInterfereRequest> request){

        this->taskSource = NULL;
        this->schema = NULL;
        this->fragment = NULL;
        this->taskInterfereRequest = request;
    }





    shared_ptr<TaskSource> getTaskSource()
    {
        return this->taskSource;
    }
    shared_ptr<OutputBufferSchema> getSchema()
    {
        return this->schema;
    }
    shared_ptr<PlanFragment> getFragment()
    {
        return this->fragment;
    }
    shared_ptr<TaskInterfereRequest> getTaskInterfereRequest()
    {
        return this->taskInterfereRequest;
    }

    shared_ptr<SessionRepresentation> getSessionRepresentation()
    {
        return this->sessionRepresentation;
    }



    static string Serialize(TaskUpdateRequest taskUpdateRequest)
    {
        nlohmann::json json;
        if(taskUpdateRequest.taskSource == NULL)
            json["taskSource"] = "NULL";
        else
            json["taskSource"] = TaskSource::Serialize(*taskUpdateRequest.taskSource);


        if(taskUpdateRequest.schema == NULL)
            json["schema"] = "NULL";
        else
            json["schema"] = OutputBufferSchema::Serialize(*taskUpdateRequest.schema);


        if(taskUpdateRequest.fragment == NULL)
            json["fragment"] = "NULL";
        else
            json["fragment"] = PlanFragment::Serialize(*taskUpdateRequest.fragment);

        if(taskUpdateRequest.taskInterfereRequest == NULL)
            json["taskInterfere"] = "NULL";
        else
            json["taskInterfere"] = TaskInterfereSerializer::Serialize(taskUpdateRequest.taskInterfereRequest);

        if(taskUpdateRequest.sessionRepresentation == NULL)
            json["sessionRepresentation"] = "NULL";
        else
            json["sessionRepresentation"] = SessionRepresentation::Serialize(*taskUpdateRequest.sessionRepresentation);



        string result = json.dump();


        return result;
    }
    static shared_ptr<TaskUpdateRequest> Deserialize(string taskUpdateRequest)
    {
        nlohmann::json json = nlohmann::json::parse(taskUpdateRequest);

        return make_shared<TaskUpdateRequest>(TaskSource::Deserialize(json["taskSource"]),OutputBufferSchema::Deserialize(json["schema"]),
                                              PlanFragment::Deserialize(json["fragment"]),TaskInterfereSerializer::Deserialize(json["taskInterfere"]),
                                              SessionRepresentation::Deserialize(json["sessionRepresentation"]));
    }


};


#endif //OLVP_TASKUPDATEREQUEST_HPP
