//
// Created by zxk on 5/19/23.
//

#ifndef OLVP_PROJECTDESCRIPTOR_HPP
#define OLVP_PROJECTDESCRIPTOR_HPP


#include "arrow/api.h"
//#include "../ProjectorAndFilter/ExprAstProjectorComplier.hpp"
//#include "../Nodes/Node/ExprOuput.hpp"
#include "FieldDesc.hpp"

#include "../Frontend/AstNodes/AstNodePtr.hpp"
#include "../Frontend/AstNodes/Serial/NodeTreeDeserialization.hpp"
#include "../Frontend/AstNodes/Serial/NodeTreeSerialization.hpp"


class ProjectAssignments
{
    vector<pair<FieldDesc,FieldDesc>> assignments;
    vector<std::shared_ptr<AstNodePtr>> exprOperations;

public:
    ProjectAssignments(){}
    ProjectAssignments(vector<pair<FieldDesc,FieldDesc>> assignments,vector<Node*> exprOperations)
    {
        this->assignments = assignments;

        vector<std::shared_ptr<AstNodePtr>> exprOps;
        for (auto op: exprOperations)
        {
            exprOps.push_back(std::make_shared<AstNodePtr>(op));
        }
        this->exprOperations = exprOps;

    }

    ProjectAssignments(vector<pair<FieldDesc,FieldDesc>> assignments,vector<std::shared_ptr<AstNodePtr>>  exprOperations)
    {
        this->assignments = assignments;
        this->exprOperations = exprOperations;

    }


    void addAssignment(FieldDesc fieldBefore,FieldDesc fieldAfter,Node * exprOperation)
    {
        assignments.push_back(make_pair(fieldBefore,fieldAfter));
        exprOperations.push_back(std::make_shared<AstNodePtr>(exprOperation));
    }

    vector<pair<FieldDesc,FieldDesc>> getAssignments()
    {
        return this->assignments;
    }
    vector<std::shared_ptr<AstNodePtr>> getExprOperations()
    {
        return this->exprOperations;
    }


    bool getProjections(vector<pair<FieldDesc,FieldDesc>> &selected_assignments,vector<std::shared_ptr<AstNodePtr>> &selected_exprOperations)
    {
        if(this->assignments.size() != this->exprOperations.size()) {
            spdlog::critical("Project assigments do not equal to operations!");
            return false;
        }
        for(int i = 0 ; i < this->assignments.size() ; i++)
        {
           selected_assignments.push_back(this->assignments[i]);
           selected_exprOperations.push_back(this->exprOperations[i]);
        }

        return true;
    }

    bool getReserveProjections(vector<pair<FieldDesc,FieldDesc>> &selected_assignments,vector<std::shared_ptr<AstNodePtr>> &selected_exprOperations)
    {
        if(this->assignments.size() != this->exprOperations.size()) {
            spdlog::critical("Project assigments do not equal to operations!");
            return false;
        }
        for(int i = 0 ; i < this->assignments.size() ; i++)
        {
            if(this->exprOperations[i]->get() != NULL && this->assignments[i].second.isEmpty())
            {
                spdlog::critical("Project assigments output is empty,but operation is not NULL! What is this expression mean?");
                return false;
            }
            if(!this->assignments[i].second.isEmpty()) {
                selected_assignments.push_back(this->assignments[i]);
                selected_exprOperations.push_back(this->exprOperations[i]);
            }
        }
        return true;
    }

    bool isRawAssignment(int index)
    {
        return this->exprOperations[index]->get() == NULL;
    }
    bool isProjectedAssignment(int index)
    {
        return this->exprOperations[index]->get() != NULL;
    }

    bool getComputationProjections(vector<pair<FieldDesc,FieldDesc>> &selected_assignments,vector<std::shared_ptr<AstNodePtr>> &selected_exprOperations)
    {
        if(this->assignments.size() != this->exprOperations.size()) {
            spdlog::critical("Project assigments do not equal to operations!");
            return false;
        }
        for(int i = 0 ; i < this->assignments.size() ; i++) {
            if (this->exprOperations[i]->get() != NULL) {
                selected_assignments.push_back(this->assignments[i]);
                selected_exprOperations.push_back(this->exprOperations[i]);
            }
        }
        return true;

    }

    bool getDeleteProjections(vector<pair<FieldDesc,FieldDesc>> &selected_assignments,vector<std::shared_ptr<AstNodePtr>> &selected_exprOperations)
    {
        if(this->assignments.size() != this->exprOperations.size()) {
            spdlog::critical("Project assigments do not equal to operations!");
            return false;
        }
        for(int i = 0 ; i < this->assignments.size() ; i++)
        {
            if(!this->assignments[i].first.equals(this->assignments[i].second))
                if(this->assignments[i].second.isEmpty()) {
                    selected_assignments.push_back(this->assignments[i]);
                    selected_exprOperations.push_back(this->exprOperations[i]);
                }
        }

        return true;
    }

    bool getRenameProjections(vector<pair<FieldDesc,FieldDesc>> &selected_assignments,vector<std::shared_ptr<AstNodePtr>> &selected_exprOperations)
    {
        if(this->assignments.size() != this->exprOperations.size()) {
            spdlog::critical("Project assigments do not equal to operations!");
            return false;
        }
        for(int i = 0 ; i < this->assignments.size() ; i++)
        {
            if(!this->assignments[i].first.equals(this->assignments[i].second))
                if(!this->assignments[i].second.isEmpty() && this->exprOperations[i] == NULL) {
                    selected_assignments.push_back(this->assignments[i]);
                    selected_exprOperations.push_back(this->exprOperations[i]);
                }
        }
        return true;
    }



    static string Serialize(ProjectAssignments projectAssignments)
    {
        nlohmann::json desc;



        vector<pair<string,string>> assignmentsString;
        for(int i = 0 ; i < projectAssignments.assignments.size() ; i++)
        {
            pair<string,string> pairString;
            pairString.first = FieldDesc::Serialize(projectAssignments.assignments[i].first);
            pairString.second = FieldDesc::Serialize(projectAssignments.assignments[i].second);
            assignmentsString.push_back(pairString);
        }

        desc["assignments"] = assignmentsString;

        AstNodeTreeSerializer serializer;

        vector<string> exprStrs;
        for(int i = 0 ; i < projectAssignments.exprOperations.size() ; i++) {
            if(projectAssignments.exprOperations[i]->get() == NULL)
            {
                exprStrs.push_back("NULL");
                continue;
            }
            string exprString = serializer.Serialize(projectAssignments.exprOperations[i]);
            exprStrs.push_back(exprString);
        }

        desc["exprOperations"] = exprStrs;

        string result = desc.dump();

        return result;
    }




    static ProjectAssignments Deserialize(string desc)
    {

        AstNodeTreeDeserializer deserializer;

        nlohmann::json projectAssignments = nlohmann::json::parse(desc);

        vector<pair<string,string>> assignmentsString = projectAssignments["assignments"];

        vector<pair<FieldDesc,FieldDesc>> assignments;


        for(int i = 0 ; i < assignmentsString.size() ; i++)
        {
            FieldDesc fieldDescFirst = FieldDesc::Deserialize(assignmentsString[i].first);
            FieldDesc fieldDescSecond = FieldDesc::Deserialize(assignmentsString[i].second);

            pair<FieldDesc,FieldDesc> assign = pair<FieldDesc,FieldDesc>(fieldDescFirst,fieldDescSecond);
            assignments.push_back(assign);
        }
        vector<string> exprStrs;
        vector<std::shared_ptr<AstNodePtr>> exprs;

        exprStrs = projectAssignments["exprOperations"];

        for(int  i = 0 ; i < exprStrs.size() ; i++)
        {
            if(exprStrs[i] == "NULL")
            {
                std::shared_ptr<AstNodePtr> astNodePtr = std::make_shared<AstNodePtr>();
                exprs.push_back(astNodePtr);
            }
            else {
                shared_ptr<AstNodePtr> astNodePtr = deserializer.Deserialize(exprStrs[i]);
                exprs.push_back(astNodePtr);
            }
        }



        return ProjectAssignments(assignments,exprs);
    }


    void test()
    {



        Int64Literal *suppkeyValue = new Int64Literal("0","123");
        Column *col = new Column("0","s_suppkey","int64");

        FunctionCall *funcName = new FunctionCall("0","multiply","int64");
        funcName->addChilds({col,suppkeyValue});


        StringLiteral *snameValue = new StringLiteral("0","_zxk");
        Column *col2 = new Column("0","s_name","string");
        FunctionCall *funcName2 = new FunctionCall("0","concat","string");
        funcName2->addChilds({col2,snameValue});

        Column *colNation = new Column("0","s_suppkey","int64");
        Column *colSupp = new Column("0","s_nationkey","int64");
        FunctionCall *funcName3 = new FunctionCall("0","multiply","int64");
        funcName3->addChilds({colNation,colSupp});

        Column *colNation2 = new Column("0","s_suppkey","int64");
        Column *colSupp2 = new Column("0","s_nationkey","int64");
        FunctionCall *funcName4 = new FunctionCall("0","subtract","int64");
        funcName4->addChilds({colNation2,colSupp2});


        Column *colAddress = new Column("0","s_address","string");
        Column *colAcctbal = new Column("0","s_acctbal","double");



        ProjectAssignments assignments;
        assignments.addAssignment(FieldDesc("s_suppkey","int64"),FieldDesc("s_suppkey","int64"),funcName);
        assignments.addAssignment(FieldDesc("s_name","string"),FieldDesc("s_name","string"),funcName2);
        assignments.addAssignment(FieldDesc("s_address","string"),FieldDesc("s_address","string"),colAddress);
        assignments.addAssignment(FieldDesc("s_nationkey","int64"),FieldDesc("s_nationkey2","int64"),funcName3);
        assignments.addAssignment(FieldDesc("s_nationkey","int64"),FieldDesc("s_nationkey3","int64"),funcName4);
        assignments.addAssignment(FieldDesc("s_phone","string"),FieldDesc("s_phone","string"),NULL);
        assignments.addAssignment(FieldDesc("s_acctbal","double"),FieldDesc("s_acctbal","double"),colAcctbal);
        assignments.addAssignment(FieldDesc("s_comment","string"),FieldDesc("s_comment","string"),NULL);



        string ass = ProjectAssignments::Serialize(assignments);

        cout << ass << endl;

        ProjectAssignments a = ProjectAssignments::Deserialize(ass);


        cout << ProjectAssignments::Serialize(a) << endl;

        cout << "----------------------------------"<<endl;


    }


};

#endif //OLVP_PROJECTDESCRIPTOR_HPP
