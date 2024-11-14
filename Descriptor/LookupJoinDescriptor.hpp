//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_LOOKUPJOINDESCRIPTOR_HPP
#define OLVP_LOOKUPJOINDESCRIPTOR_HPP


#include "FieldDesc.hpp"

#include <string>
#include <vector>
#include "arrow/api.h"
#include "../Utils/ArrowDicts.hpp"

using  namespace  std;

class LookupJoinDescriptor
{
    vector<FieldDesc> probeInputSchema;
    vector<FieldDesc> buildInputSchema;

    vector<FieldDesc> buildOutputSchema;

    vector<int> probeOutputChannels;
    vector<int> probeHashChannels;

    vector<int> buildOutputChannels;
    vector<int> buildHashChannels;


public:
    LookupJoinDescriptor(){}
    LookupJoinDescriptor(vector<FieldDesc> probeInputSchema, vector<int> probeHashChannels,vector<int> probeOutputChannels,
                         vector<FieldDesc> buildInputSchema,vector<int> buildHashChannels,vector<int> buildOutputChannels,
                         vector<FieldDesc> buildOutputSchema)
    {
        this->probeInputSchema = probeInputSchema;
        this->buildInputSchema = buildInputSchema;
        this->probeOutputChannels = probeOutputChannels;
        this->probeHashChannels = probeHashChannels;
        this->buildOutputChannels = buildOutputChannels;
        this->buildHashChannels = buildHashChannels;
        this->buildOutputSchema = buildOutputSchema;
    }


    vector<FieldDesc> getProbeInputSchema(){
        return this->probeInputSchema;
    }
    std::shared_ptr<arrow::Schema> getProbeInputArrowSchema()
    {
        arrow::FieldVector fields;
        vector<FieldDesc> vSchema = this->probeInputSchema;

        for(int i = 0 ; i < vSchema.size() ; i++)
        {
            fields.push_back(arrow::field(vSchema[i].getFieldName(),Typer::getType(vSchema[i].getFieldType())));
        }

        return arrow::schema(fields);
    }

    vector<FieldDesc> getBuildInputSchema(){
        return this->buildInputSchema;
    }


    std::shared_ptr<arrow::Schema> getBuildInputArrowSchema()
    {
        arrow::FieldVector fields;
        vector<FieldDesc> vSchema = this->buildInputSchema;

        for(int i = 0 ; i < vSchema.size() ; i++)
        {
            fields.push_back(arrow::field(vSchema[i].getFieldName(),Typer::getType(vSchema[i].getFieldType())));
        }

        return arrow::schema(fields);
    }



    vector<FieldDesc> getBuildOutputSchema(){
        return this->buildOutputSchema;
    }



    std::shared_ptr<arrow::Schema> getBuildOutputArrowSchema()
    {
        arrow::FieldVector fields;
        vector<FieldDesc> vSchema = this->buildOutputSchema;

        for(int i = 0 ; i < vSchema.size() ; i++)
        {
            fields.push_back(arrow::field(vSchema[i].getFieldName(),Typer::getType(vSchema[i].getFieldType())));
        }

        return arrow::schema(fields);
    }





    vector<int> getProbeOutputChannels(){
        return this->probeOutputChannels;
    }
    vector<int> getProbeHashChannels(){
        return this->probeHashChannels;
    }

    vector<int> getBuildOutputChannels(){
        return this->buildOutputChannels;
    }
    vector<int> getBuildHashChannels(){
        return this->buildHashChannels;
    }





    static string Serialize(LookupJoinDescriptor lookupJoinDescriptor)
    {
        nlohmann::json desc;


        desc["probeInputSchema"] = FieldDesc::SerializeObjects(lookupJoinDescriptor.probeInputSchema);
        desc["buildInputSchema"] = FieldDesc::SerializeObjects(lookupJoinDescriptor.buildInputSchema);
        desc["buildOutputSchema"] = FieldDesc::SerializeObjects(lookupJoinDescriptor.buildOutputSchema);
        desc["probeOutputChannels"] = lookupJoinDescriptor.probeOutputChannels;
        desc["probeHashChannels"] = lookupJoinDescriptor.probeHashChannels;
        desc["buildOutputChannels"] = lookupJoinDescriptor.buildOutputChannels;
        desc["buildHashChannels"] = lookupJoinDescriptor.buildHashChannels;


        string result = desc.dump();

        return result;
    }
    static LookupJoinDescriptor Deserialize(string desc)
    {
        nlohmann::json lookupJoinDescriptor = nlohmann::json::parse(desc);



        vector<FieldDesc> probeInputSchema = FieldDesc::DeserializeObjects(lookupJoinDescriptor["probeInputSchema"]);
        vector<int> probeHashChannels = lookupJoinDescriptor["probeHashChannels"];
        vector<int> probeOutputChannels = lookupJoinDescriptor["probeOutputChannels"];
        vector<FieldDesc> buildInputSchema =  FieldDesc::DeserializeObjects(lookupJoinDescriptor["buildInputSchema"]);
        vector<int> buildHashChannels = lookupJoinDescriptor["buildHashChannels"];
        vector<int> buildOutputChannels = lookupJoinDescriptor["buildOutputChannels"];
        vector<FieldDesc> buildOutputSchema =  FieldDesc::DeserializeObjects(lookupJoinDescriptor["buildOutputSchema"]);


        return LookupJoinDescriptor(probeInputSchema,probeHashChannels,probeOutputChannels,buildInputSchema,buildHashChannels,buildOutputChannels,buildOutputSchema);
    }


    void test()
    {



        vector<FieldDesc> inputProbeSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_address","string"),FieldDesc("s_nationkey","int64"),FieldDesc("s_phone","string"),FieldDesc("s_acctbal","double"),FieldDesc("s_comment","string")};
        vector<FieldDesc> inputBuildSchema = {FieldDesc("s_suppkey2","int64"),FieldDesc("s_name2","string"),FieldDesc("s_address2","string"),FieldDesc("s_nationkey2","int64"),FieldDesc("s_phone2","string"),FieldDesc("s_acctbal2","double"),FieldDesc("s_comment2","string")};

        vector<FieldDesc> outputSchema = {FieldDesc("s_suppkey2","int64"),FieldDesc("s_name2","string"),FieldDesc("s_nationkey2","int64")};



        vector<int> tprobeOutputChannels = {0,3};
        vector<int> tprobeHashChannels = {3};

        vector<int> tbuildOutputChannels = {0,3};
        vector<int> tbuildHashChannels = {3};



        LookupJoinDescriptor descriptor(inputProbeSchema,tprobeHashChannels,tprobeOutputChannels,inputBuildSchema,tbuildOutputChannels,tbuildHashChannels,outputSchema);

        string desc = LookupJoinDescriptor::Serialize(descriptor);
        cout << desc << endl;

        LookupJoinDescriptor descriptor1 = LookupJoinDescriptor::Deserialize(desc);

        cout << LookupJoinDescriptor::Serialize(descriptor1)<<endl;

        cout << "----------------------------------"<<endl;

    }


};





#endif //OLVP_LOOKUPJOINDESCRIPTOR_HPP
