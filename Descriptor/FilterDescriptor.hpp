//
// Created by zxk on 5/20/23.
//

#ifndef OLVP_FILTERDESCRIPTOR_HPP
#define OLVP_FILTERDESCRIPTOR_HPP

#include "FieldDesc.hpp"
#include "../ProjectorAndFilter/ExprAstFilterComplier.hpp"
#include "../ProjectorAndFilter/ExprAstFilter.hpp"
#include "../Frontend/AstNodes/AstNodePtr.hpp"
#include "../Frontend/AstNodes/Serial/NodeTreeSerialization.hpp"
#include "../Frontend/AstNodes/Serial/NodeTreeDeserialization.hpp"

class FilterDescriptor
{
    vector<FieldDesc> inputFields;
    std::shared_ptr<AstNodePtr> filterExpr;

public:
    FilterDescriptor(){}
    FilterDescriptor(vector<FieldDesc> inputFields,Node* filterExprTree)
    {
        this->inputFields = inputFields;
        this->filterExpr = std::make_shared<AstNodePtr>(filterExprTree);
    }

    FilterDescriptor(vector<FieldDesc> inputFields,std::shared_ptr<AstNodePtr> filterExprTree)
    {
        this->inputFields = inputFields;
        this->filterExpr = filterExprTree;
    }
    vector<FieldDesc> getInputFields()
    {
        return this->inputFields;
    }
    std::shared_ptr<AstNodePtr>  getFilterExpr()
    {
        return this->filterExpr;
    }
    void setExpr(Node* filterExprTree)
    {
        this->filterExpr = std::make_shared<AstNodePtr>(filterExprTree);
    }

    void setExpr(std::shared_ptr<AstNodePtr> filterExprTree)
    {
        this->filterExpr = filterExprTree;
    }


    void addField(FieldDesc field)
    {
        this->inputFields.push_back(field);
    }


    static string Serialize(FilterDescriptor filterDescriptor)
    {
        nlohmann::json desc;

        vector<FieldDesc> fields = filterDescriptor.inputFields;

        nlohmann::json fieldsJson;
        for(int i = 0  ; i < fields.size() ; i++)
        {
            fieldsJson.push_back(FieldDesc::Serialize(fields[i]));
        }

        AstNodeTreeSerializer serializer;
        string filterExprTreeString = serializer.Serialize(filterDescriptor.filterExpr->get());

        desc["inputFields"] = fieldsJson;
        desc["filterExpr"] = filterExprTreeString;

        string result = desc.dump();

        return result;
    }
    static FilterDescriptor Deserialize(string desc)
    {
        nlohmann::json filter = nlohmann::json::parse(desc);

        nlohmann::json fields = nlohmann::json::array();

        fields = filter["inputFields"];

        vector<FieldDesc> fieldDescs;
        for(auto field : fields)
        {
            FieldDesc fd = FieldDesc::Deserialize(field);
            fieldDescs.push_back(fd);
        }


        AstNodeTreeDeserializer deserializer;

        std::shared_ptr<AstNodePtr> astNodePtr = deserializer.Deserialize(filter["filterExpr"]);

        return FilterDescriptor(fieldDescs,astNodePtr);
    }


    void test()
    {
        FilterDescriptor FDesc;
        FDesc.addField(FieldDesc("s_suppkey","int64"));
        FDesc.addField(FieldDesc("s_name","string"));
        FDesc.addField(FieldDesc("s_address","string"));
        FDesc.addField(FieldDesc("s_nationkey2","int64"));
        FDesc.addField(FieldDesc("s_nationkey3","int64"));
        FDesc.addField(FieldDesc("s_acctbal","double"));


        Column *colAcc = new Column("0","s_acctbal","double");
        DoubleLiteral *comValue = new DoubleLiteral("0","8888");
        FunctionCall *accCom = new FunctionCall("0","greater_than","bool");
        accCom->addChilds({colAcc,comValue});

        FDesc.setExpr(accCom);

        string fdesc = FilterDescriptor::Serialize(FDesc);

        cout << fdesc<<endl;

        FilterDescriptor descriptor = FilterDescriptor::Deserialize(fdesc);

        cout << FilterDescriptor::Serialize(descriptor) << endl;

        cout << "----------------------------------"<<endl;

    }



};



#endif //OLVP_FILTERDESCRIPTOR_HPP
