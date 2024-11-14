//
// Created by zxk on 6/28/23.
//

#ifndef OLVP_CROSSJOINDESCRIPTOR_HPP
#define OLVP_CROSSJOINDESCRIPTOR_HPP



#include "FieldDesc.hpp"

#include <string>
#include <vector>
#include "arrow/api.h"
#include "../Utils/ArrowDicts.hpp"
#include "nlohmann/json.hpp"

using  namespace  std;

class CrossJoinDescriptor {
    vector<FieldDesc> probeInputSchema;
    vector<FieldDesc> buildInputSchema;
    vector<FieldDesc> probeOutputSchema;
    vector<FieldDesc> buildOutputSchema;


public:
    CrossJoinDescriptor() {}

    CrossJoinDescriptor(vector<FieldDesc> probeInputSchema, vector<FieldDesc> buildInputSchema,vector<FieldDesc> probeOutputSchema,vector<FieldDesc> buildOutputSchema) {
        this->probeInputSchema = probeInputSchema;
        this->buildInputSchema = buildInputSchema;
        this->probeOutputSchema = probeOutputSchema;
        this->buildOutputSchema = buildOutputSchema;

    }


    vector<FieldDesc> getProbeInputSchema() {
        return this->probeInputSchema;
    }

    std::shared_ptr<arrow::Schema> getProbeInputArrowSchema() {
        arrow::FieldVector fields;
        vector<FieldDesc> vSchema = this->probeInputSchema;

        for (int i = 0; i < vSchema.size(); i++) {
            fields.push_back(arrow::field(vSchema[i].getFieldName(), Typer::getType(vSchema[i].getFieldType())));
        }

        return arrow::schema(fields);
    }

    vector<FieldDesc> getBuildInputSchema() {
        return this->buildInputSchema;
    }


    std::shared_ptr<arrow::Schema> getBuildInputArrowSchema() {
        arrow::FieldVector fields;
        vector<FieldDesc> vSchema = this->buildInputSchema;

        for (int i = 0; i < vSchema.size(); i++) {
            fields.push_back(arrow::field(vSchema[i].getFieldName(), Typer::getType(vSchema[i].getFieldType())));
        }

        return arrow::schema(fields);
    }



    vector<FieldDesc> getProbeOutputSchema() {
        return this->probeOutputSchema;
    }

    std::shared_ptr<arrow::Schema> getProbeOutputArrowSchema() {
        arrow::FieldVector fields;
        vector<FieldDesc> vSchema = this->probeOutputSchema;

        for (int i = 0; i < vSchema.size(); i++) {
            fields.push_back(arrow::field(vSchema[i].getFieldName(), Typer::getType(vSchema[i].getFieldType())));
        }

        return arrow::schema(fields);
    }

    vector<FieldDesc> getBuildOutputSchema() {
        return this->buildOutputSchema;
    }


    std::shared_ptr<arrow::Schema> getBuildOutputArrowSchema() {
        arrow::FieldVector fields;
        vector<FieldDesc> vSchema = this->buildOutputSchema;

        for (int i = 0; i < vSchema.size(); i++) {
            fields.push_back(arrow::field(vSchema[i].getFieldName(), Typer::getType(vSchema[i].getFieldType())));
        }

        return arrow::schema(fields);
    }

    static string Serialize(CrossJoinDescriptor crossJoinDescriptor) {
        nlohmann::json desc;


        desc["probeInputSchema"] = FieldDesc::SerializeObjects(crossJoinDescriptor.probeInputSchema);
        desc["buildInputSchema"] = FieldDesc::SerializeObjects(crossJoinDescriptor.buildInputSchema);
        desc["probeOutputSchema"] = FieldDesc::SerializeObjects(crossJoinDescriptor.probeOutputSchema);
        desc["buildOutputSchema"] = FieldDesc::SerializeObjects(crossJoinDescriptor.buildOutputSchema);


        string result = desc.dump();

        return result;
    }

    static CrossJoinDescriptor Deserialize(string desc) {
        nlohmann::json crossJoinDescriptor = nlohmann::json::parse(desc);

        vector<FieldDesc> probeInputSchema = FieldDesc::DeserializeObjects(crossJoinDescriptor["probeInputSchema"]);
        vector<FieldDesc> buildInputSchema = FieldDesc::DeserializeObjects(crossJoinDescriptor["buildInputSchema"]);
        vector<FieldDesc> probeOutputSchema = FieldDesc::DeserializeObjects(crossJoinDescriptor["probeOutputSchema"]);
        vector<FieldDesc> buildOutputSchema = FieldDesc::DeserializeObjects(crossJoinDescriptor["buildOutputSchema"]);

        return CrossJoinDescriptor(probeInputSchema, buildInputSchema,probeOutputSchema,buildOutputSchema);
    }


};
#endif //OLVP_CROSSJOINDESCRIPTOR_HPP
