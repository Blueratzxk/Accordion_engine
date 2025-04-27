//
// Created by zxk on 4/20/25.
//

#ifndef OLVP_INTERTASKMISSIONDESCRIPTOR_HPP
#define OLVP_INTERTASKMISSIONDESCRIPTOR_HPP


class InterTaskMissionDescriptor
{

    string missionType = "";
    string taskId = "";
    string componentId = "";
    string IP = "";
    string PORT = "";
    string bufferId = "";
public:
    InterTaskMissionDescriptor(string missionType,string componentId){
        this->missionType = missionType;
        this->componentId = componentId;
    }

    InterTaskMissionDescriptor(string missionType,string taskId,string componentId,string IP,string PORT,string bufferId){
        this->missionType = missionType;
        this->componentId = componentId;
        this->IP = IP;
        this->PORT = PORT;
        this->bufferId = bufferId;
        this->taskId = taskId;
    }

    string getBufferId(){return this->bufferId;}
    string getIP(){return this->IP;}
    string getPort(){return this->PORT;}
    string getMissionType(){return this->missionType;}
    string getComponentId(){return this->componentId;}
    string getTaskId(){return this->taskId;}

    static string Serialize(InterTaskMissionDescriptor interTaskMissionDescriptor) {

        nlohmann::json json;

        json["missionType"] = interTaskMissionDescriptor.missionType;
        json["componentId"] = interTaskMissionDescriptor.componentId;
        json["IP"] = interTaskMissionDescriptor.IP;
        json["PORT"] = interTaskMissionDescriptor.PORT;
        json["bufferId"] = interTaskMissionDescriptor.bufferId;
        json["taskId"] = interTaskMissionDescriptor.taskId;

        string result = json.dump();
        return result;

    }
    static shared_ptr<InterTaskMissionDescriptor> Deserialize(string interTaskMissionDescriptor)
    {
        nlohmann::json json = nlohmann::json::parse(interTaskMissionDescriptor);

        return make_shared<InterTaskMissionDescriptor>(json["missionType"],json["taskId"],json["componentId"],json["IP"],json["PORT"],json["bufferId"]);
    }


};



#endif //OLVP_INTERTASKMISSIONDESCRIPTOR_HPP
