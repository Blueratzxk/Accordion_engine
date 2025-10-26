//
// Created by zxk on 10/25/25.
//

#ifndef OLVP_JOINHASHCOMPONENT_HPP
#define OLVP_JOINHASHCOMPONENT_HPP

#include "../Utils/StringUtils.hpp"

class JoinHashComponent : public JoinBuildComponents
{
    int partitionId;
    int hashSize;
    int positionCount;

    std::shared_ptr<int> partitionToHashKeyArray;
    std::shared_ptr<uint8_t>  partitionToPositionToHashes;
    std::shared_ptr<int> partitionToPositionLinks;

    shared_ptr<arrow::Table> table;

public:

    JoinHashComponent(int partitionId, int hashSize, int positionCount): JoinBuildComponents("JoinHashComponent")
    {
        this->partitionId = partitionId;
        this->hashSize = hashSize;
        this->positionCount = positionCount;
    }

    JoinHashComponent() : JoinBuildComponents("JoinHashComponent"){

    }

    int getPartitionId()
    {
        return this->partitionId;
    }

    void setTable(shared_ptr<arrow::Table> table)
    {
        this->table = table;
    }
    void addPartitionToHashKeyArray(std::shared_ptr<int> keyArrays)
    {
        this->partitionToHashKeyArray = keyArrays;
    }
    void addParitionToPositionToHashes(std::shared_ptr<uint8_t> pts)
    {
        this->partitionToPositionToHashes = pts;
    }
    void addPartitionToPositionLinks(std::shared_ptr<int> positionLinks)
    {
        this->partitionToPositionLinks = positionLinks;
    }

    std::shared_ptr<int> getPartitionToHashKeyArray(){return this->partitionToHashKeyArray;}
    std::shared_ptr<uint8_t>  getPartitionToPositionToHashes(){return this->partitionToPositionToHashes;}
    std::shared_ptr<int> getPartitionToPositionLinks(){return this->partitionToPositionLinks;}

    //------------------------------------------------------------------------------------------//
    shared_ptr<DataPage> partitionToHashKeyArray_TransformToRecordBatch() {
        arrow::Status status;
        vector<shared_ptr<arrow::Field>> fields;


        string fieldName;
        fieldName.append("_partitionToHashKeyArray_").append("partitionId_").append(to_string(partitionId));

        auto newField = make_shared<arrow::Field>(fieldName, arrow::int32());
        fields.push_back(newField);


        auto buffer = std::make_shared<arrow::Buffer>(
                reinterpret_cast<const uint8_t*>(this->partitionToHashKeyArray.get()),
                this->hashSize * sizeof(int32_t));

        auto array_data = arrow::ArrayData::Make(
                arrow::int32(),
                this->hashSize,
                {nullptr, buffer});

        auto array = arrow::MakeArray(array_data);


        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema,array->length(),{array});
        return make_shared<DataPage>(batch);
    }
    void recordBatch_ToPartitionToHashKeyArray(shared_ptr<DataPage> page)
    {

        auto batch = page->get();
        auto schema = batch->schema();
        auto allFields = schema->fields();

        if(allFields[0]->name().find("partitionToHashKeyArray") == std::string::npos)
            return;

        for(int i = 0 ; i < allFields.size(); i++)
        {
            vector<string> res;
            StringUtils::Stringsplit(allFields[i]->name(),'_',res);

            auto col = batch->column(0);


            this->hashSize = col->length();

            auto int32Array = std::static_pointer_cast<arrow::Int32Array>(col);
            auto buffer = int32Array->values();
            this->partitionToHashKeyArray = std::shared_ptr<int32_t>(const_cast<int32_t*>(reinterpret_cast<const int32_t*>(buffer->data())),
                    [buffer](int32_t*) { /* read only!!! */ });

        }

    }



    //------------------------------------------------------------------------------------------//



    shared_ptr<DataPage> partitionToPositionToHashes_TransformToRecordBatch() {
        arrow::Status status;
        vector<shared_ptr<arrow::Field>> fields;

        vector<shared_ptr<arrow::Array>> partitionToKeyArrays;


        string fieldName;
        fieldName.append("_partitionToPositionToHashes_").append("partitionId_").append(to_string(partitionId));

        auto newField = make_shared<arrow::Field>(fieldName, arrow::int32());
        fields.push_back(newField);


        auto buffer = std::make_shared<arrow::Buffer>(
                reinterpret_cast<const uint8_t*>(this->partitionToPositionToHashes.get()),
                this->hashSize * sizeof(uint8_t));

        auto array_data = arrow::ArrayData::Make(
                arrow::uint8(),
                this->hashSize,
                {nullptr, buffer});

        auto array = arrow::MakeArray(array_data);


        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});
        return make_shared<DataPage>(batch);
    }
    void recordBatch_ToPartitionToPositionToHashes(shared_ptr<DataPage> page)
    {

        auto batch = page->get();
        auto schema = batch->schema();
        auto allFields = schema->fields();

        if(allFields[0]->name().find("partitionToPositionToHashes") == std::string::npos)
            return;


        for(int i = 0 ; i < allFields.size(); i++)
        {
            vector<string> res;
            StringUtils::Stringsplit(allFields[i]->name(),'_',res);
            string partitionId = res.back();

            auto col = batch->column(0);
            this->partitionToPositionToHashes =  shared_ptr<uint8_t> (new uint8_t[col->length()],[](uint8_t *p){delete [] p;});

            this->hashSize = col->length();

            auto uint8Array = std::static_pointer_cast<arrow::UInt8Array>(col);
            auto buffer = uint8Array->values();
            this->partitionToPositionToHashes = std::shared_ptr<uint8_t>(const_cast<uint8_t*>(buffer->data()),
                                                                         [buffer](uint8_t*) {/*read only!!!*/});
        }

    }


    //------------------------------------------------------------------------------------------//

    shared_ptr<DataPage> partitionToPositionLinks_TransformToRecordBatch() {
        arrow::Status status;
        vector<shared_ptr<arrow::Field>> fields;

        vector<shared_ptr<arrow::Array>> partitionToKeyArrays;


        string fieldName;
        fieldName.append("_partitionToPositionLinks_").append("partitionId_").append(to_string(partitionId));

        auto newField = make_shared<arrow::Field>(fieldName, arrow::int32());
        fields.push_back(newField);


        auto buffer = std::make_shared<arrow::Buffer>(
                reinterpret_cast<const uint8_t*>(this->partitionToPositionLinks.get()),
                this->positionCount * sizeof(int32_t));

        auto array_data = arrow::ArrayData::Make(
                arrow::int32(),
                this->positionCount,
                {nullptr, buffer});

        auto array = arrow::MakeArray(array_data);


        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema,array->length(), {array});
        return make_shared<DataPage>(batch);
    }
    void recordBatch_ToPartitionToPositionLinks(shared_ptr<DataPage> page)
    {

        auto batch = page->get();
        auto schema = batch->schema();
        auto allFields = schema->fields();

        if(allFields[0]->name().find("partitionToPositionLinks") == std::string::npos)
            return;


        for(int i = 0 ; i < allFields.size(); i++)
        {
            vector<string> res;
            StringUtils::Stringsplit(allFields[i]->name(),'_',res);
            string partitionId = res.back();

            auto col = batch->column(0);

            this->positionCount = col->length();


            auto int32Array = std::static_pointer_cast<arrow::Int32Array>(col);

            auto buffer = int32Array->values();
            this->partitionToPositionLinks = std::shared_ptr<int32_t>(const_cast<int32_t*>(reinterpret_cast<const int32_t*>(buffer->data())),
                                                                     [buffer](int32_t*) { /* read only!!! */ });




        }

    }


    //------------------------------------------------------------------------------------------//



    shared_ptr<DataPage> tables_TransformToRecordBatch() {


        auto fields = table->schema()->fields();
        vector<string> colNames;
        for (auto field: fields)
            colNames.push_back(field->name());
        for (auto &cn: colNames) {
            cn.append("_HashJoinBuildComponentsTables_partitionId_" + to_string(partitionId));
        }
        auto newTable = renameTableSchema(table, colNames);
        auto recordBatch = newTable->CombineChunksToBatch().ValueOrDie();
        return make_shared<DataPage>(recordBatch);

    }


    void recordBatch_ToTables(shared_ptr<DataPage> page)
    {

        auto batch = page->get();
        auto schema = batch->schema();
        auto allFields = schema->fields();

        for(auto field : allFields)
            spdlog::info(field->name());

        if(allFields[0]->name().find("_HashJoinBuildComponentsTables") == std::string::npos)
            return;




        vector<string> trueFields;

        for(int i = 0 ; i < allFields.size(); i++)
        {
            vector<string> res;

            int pos = allFields[i]->name().find("_HashJoinBuildComponentsTables");


            string trueFieldName = allFields[i]->name().substr(0,pos);

            trueFields.push_back(trueFieldName);
        }
        auto table = arrow::Table::FromRecordBatches({batch});
        auto newTable = renameTableSchema(table.ValueOrDie(),trueFields);

        this->table = newTable;
    }




    std::shared_ptr<arrow::Table> renameTableSchema(
            const std::shared_ptr<arrow::Table>& table,
            const std::vector<std::string>& new_names) {

        // 确保新名字数量与旧字段数量相同
        if (new_names.size() != static_cast<size_t>(table->num_columns())) {
            throw std::runtime_error("new_names count != column count");
        }

        std::vector<std::shared_ptr<arrow::Field>> new_fields;
        auto old_fields = table->schema()->fields();

        for (size_t i = 0; i < new_names.size(); ++i) {
            new_fields.push_back(arrow::field(new_names[i],old_fields[i]->type()));
        }

        auto new_schema = arrow::schema(new_fields);

        // 创建新 Table（数据不变）
        auto new_table = arrow::Table::Make(new_schema, table->columns());
        return new_table;
    }

    vector<shared_ptr<DataPage>> ToDataPages() override
    {
        vector<shared_ptr<DataPage>> pages;

        spdlog::info("partitionToHashKeyArray_TransformToRecordBatch");
        auto re = partitionToHashKeyArray_TransformToRecordBatch();
        pages.push_back(re);

        spdlog::info("partitionToPositionLinks_TransformToRecordBatch");
        re = partitionToPositionLinks_TransformToRecordBatch();
        pages.push_back(re);

        spdlog::info("partitionToPositionToHashes_TransformToRecordBatch");
        re = partitionToPositionToHashes_TransformToRecordBatch();
        pages.push_back(re);

        spdlog::info("tables_TransformToRecordBatch");
        re = tables_TransformToRecordBatch();
        pages.push_back(re);

        return pages;
    }

    int getHashSize()
    {
        return this->hashSize;
    }
    int getPositionCount()
    {
        return this->positionCount;
    }

    shared_ptr<DataPage> getTable()
    {
        auto batch = this->table->CombineChunksToBatch();
        return make_shared<DataPage>(batch.ValueOrDie());
    }


    void fromDataPages(vector<shared_ptr<DataPage>> pages) override
    {
        for(auto page : pages)
        {
            recordBatch_ToTables(page);
            recordBatch_ToPartitionToPositionLinks(page);
            recordBatch_ToPartitionToHashKeyArray(page);
            recordBatch_ToPartitionToPositionToHashes(page);
        }
    }

    int getComponentPartitionId(shared_ptr<DataPage> page)
    {
        auto batch = page->get();
        auto schema = batch->schema();
        auto allFields = schema->fields();

        string fieldName = allFields[0]->name();

        vector<string> strs;
        StringUtils::Stringsplit(fieldName,'_',strs);

        auto partitionIdStr = strs.back();
        return atoi(partitionIdStr.c_str());
    }


};



#endif //OLVP_JOINHASHCOMPONENT_HPP
