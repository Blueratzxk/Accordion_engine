//
// Created by zxk on 10/25/25.
//

#ifndef OLVP_SIDEWAYMISSIONHANDLE_HPP
#define OLVP_SIDEWAYMISSIONHANDLE_HPP



class InterTaskDataHandle
{
    bool status;
    string message;


    shared_ptr<SidewayPreparationResponse> sidewayPreparationResponse;
public:
    InterTaskDataHandle()
    {
        this->status = true;
    }
    InterTaskDataHandle(bool status, string message) {
        this->status = status;
        this->message = message;
    }

    InterTaskDataHandle(bool status, string message, shared_ptr<SidewayPreparationResponse> sidewayPreparationResponse)
    {
        this->status = status;
        this->message = message;
        this->sidewayPreparationResponse = sidewayPreparationResponse;
    }

    bool getStatus(){return this->status;}
    shared_ptr<SidewayPreparationResponse> getSidewayPreparationResponse(){return this->sidewayPreparationResponse;}
    string getMessage(){return this->message;}

    static string Serialize(InterTaskDataHandle interTaskDataHandle)
    {
        nlohmann::json json;

        json["status"] = interTaskDataHandle.status;

        json["sidewayPreparationResponse"] = SidewayPreparationResponse::Serialize(*interTaskDataHandle.sidewayPreparationResponse);

        json["message"] = interTaskDataHandle.message;

        string result = json.dump();
        return result;
    }

    static shared_ptr<InterTaskDataHandle> Deserialize(string interTaskDataHandle)
    {
        nlohmann::json json = nlohmann::json::parse(interTaskDataHandle);


        auto result = make_shared<InterTaskDataHandle>(json["status"],json["message"],SidewayPreparationResponse::Deserialize(json["sidewayPreparationResponse"]));

        return  result;
    }


};



#endif //OLVP_SIDEWAYMISSIONHANDLE_HPP
