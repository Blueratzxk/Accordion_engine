//
// Created by zxk on 10/25/25.
//

#ifndef OLVP_HASHJOINBUILDCOMPONENTS_HPP
#define OLVP_HASHJOINBUILDCOMPONENTS_HPP

#include "JoinBuildComponents.hpp"
#include "../Utils/StringUtils.hpp"
class HashJoinBuildComponents : public JoinBuildComponents
{
    int positionCount = 0;
    int positionLinkSize = 0;
    int hashSize = 0;
    map<int,std::shared_ptr<int>> partitionToHashKeyArray;
    map<int,std::shared_ptr<uint8_t>>  partitionToPositionToHashes;
    map<int,std::shared_ptr<int>> partitionToPositionLinks;

    map<int,shared_ptr<arrow::Table>> tables;

public:
    HashJoinBuildComponents(int positionCount,int hashSize) : JoinBuildComponents("HashJoinBuildComponents")
    {
        this->positionCount = positionCount;
        this->hashSize = hashSize;
        this->positionLinkSize = positionCount;
    }

    void setTables( map<int,shared_ptr<arrow::Table>> tables)
    {
       this->tables = tables;
    }
    void addPartitionToHashKeyArray(int partitionId,std::shared_ptr<int> keyArrays)
    {
        this->partitionToHashKeyArray[partitionId] = keyArrays;
    }
    void addParitionToPositionToHashes(int partitionId,std::shared_ptr<uint8_t> pts)
    {
        this->partitionToPositionToHashes[partitionId] = pts;
    }
    void addPartitionToPositionLinks(int partitionId,std::shared_ptr<int> positionLinks)
    {
        this->partitionToPositionLinks[partitionId] = positionLinks;
    }

    //------------------------------------------------------------------------------------------//
    shared_ptr<DataPage> partitionToHashKeyArray_TransformToRecordBatch()
    {
        arrow::Status status;
        vector<shared_ptr<arrow::Field>> fields;

        vector<shared_ptr<arrow::Array>> partitionToKeyArrays;
        for (const auto& kv : partitionToHashKeyArray) {

            string fieldName;
            fieldName.append("partitionToHashKeyArray_").append("partitionId_").append(to_string(kv.first));

            auto newField = make_shared<arrow::Field>(fieldName,arrow::int32());
            fields.push_back(newField);


            arrow::Int32Builder key_builder;
            std::shared_ptr<arrow::Array> key_array;
            auto keyArray = kv.second;
            for(int i = 0 ;i < this->hashSize ; i++)
            {
                auto status = key_builder.Append((keyArray).get()[i]);
                if(!status.ok()) {
                    spdlog::error("HashJoinBuildComponents partitionToHashKeyArray_TransformToRecordBatch error!");
                    exit(0);
                }
            }
            auto status = key_builder.Finish(&key_array);
            if(!status.ok()) {
                spdlog::error("HashJoinBuildComponents partitionToHashKeyArray_TransformToRecordBatch error!");
                exit(0);
            }
            partitionToKeyArrays.push_back(key_array);
        }

        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema, partitionToKeyArrays[0]->length(), partitionToKeyArrays);
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
            string partitionId = res.back();
            int pid = atoi(partitionId.c_str());
            auto col = batch->column(pid);
            this->partitionToHashKeyArray[pid] = shared_ptr<int> (new int[col->length()],[](int *p){delete [] p;});

            auto int32Array = std::static_pointer_cast<arrow::Int32Array>(col);
            for(int i = 0 ; i < col->length() ; i++)
                this->partitionToHashKeyArray[pid].get()[i] = int32Array->Value(i);
        }

    }



    //------------------------------------------------------------------------------------------//



    shared_ptr<DataPage> partitionToPositionToHashes_TransformToRecordBatch()
    {
        arrow::Status status;
        vector<shared_ptr<arrow::Field>> fields;

        vector<shared_ptr<arrow::Array>> partitionToKeyArrays;
        for (const auto& kv : partitionToPositionToHashes) {

            string fieldName;
            fieldName.append("partitionToPositionToHashes_").append("partitionId_").append(to_string(kv.first));

            auto newField = make_shared<arrow::Field>(fieldName,arrow::int32());
            fields.push_back(newField);


            arrow::UInt8Builder key_builder;
            std::shared_ptr<arrow::Array> key_array;
            auto keyArray = kv.second;
            for(int i = 0 ;i < this->hashSize ; i++)
            {
                auto status = key_builder.Append((keyArray).get()[i]);
                if(!status.ok()) {
                    spdlog::error("HashJoinBuildComponents partitionToPositionToHashes_TransformToRecordBatch error!");
                    exit(0);
                }
            }
            auto status = key_builder.Finish(&key_array);
            if(!status.ok()) {
                spdlog::error("HashJoinBuildComponents partitionToPositionToHashes_TransformToRecordBatch error!");
                exit(0);
            }
            partitionToKeyArrays.push_back(key_array);
        }

        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema, partitionToKeyArrays[0]->length(), partitionToKeyArrays);
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
            int pid = atoi(partitionId.c_str());
            auto col = batch->column(pid);
            this->partitionToPositionToHashes[pid] =  shared_ptr<uint8_t> (new uint8_t[hashSize],[](uint8_t *p){delete [] p;});

            auto int32Array = std::static_pointer_cast<arrow::UInt8Array>(col);
            for(int i = 0 ; i < col->length() ; i++)
                this->partitionToPositionToHashes[pid].get()[i] = int32Array->Value(i);
        }

    }


    //------------------------------------------------------------------------------------------//

    shared_ptr<DataPage> partitionToPositionLinks_TransformToRecordBatch()
    {
        arrow::Status status;
        vector<shared_ptr<arrow::Field>> fields;

        vector<shared_ptr<arrow::Array>> partitionToKeyArrays;
        for (const auto& kv : partitionToPositionLinks) {

            string fieldName;
            fieldName.append("partitionToPositionLinks_").append("partitionId_").append(to_string(kv.first));

            auto newField = make_shared<arrow::Field>(fieldName,arrow::int32());
            fields.push_back(newField);


            arrow::Int32Builder key_builder;
            std::shared_ptr<arrow::Array> key_array;
            auto keyArray = kv.second;
            for(int i = 0 ;i < this->positionLinkSize ; i++)
            {
                auto status = key_builder.Append((keyArray).get()[i]);
                if(!status.ok()) {
                    spdlog::error("HashJoinBuildComponents partitionToPositionLinks_TransformToRecordBatch error!");
                    exit(0);
                }
            }
            auto status = key_builder.Finish(&key_array);
            if(!status.ok()) {
                spdlog::error("HashJoinBuildComponents partitionToPositionLinks_TransformToRecordBatch error!");
                exit(0);
            }
            partitionToKeyArrays.push_back(key_array);
        }

        auto schema = arrow::schema(fields);
        auto batch = arrow::RecordBatch::Make(schema, partitionToKeyArrays[0]->length(), partitionToKeyArrays);
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
            int pid = atoi(partitionId.c_str());
            auto col = batch->column(pid);
            this->partitionToPositionLinks[pid] = shared_ptr<int> (new int[col->length()],[](int *p){delete [] p;});

            auto int32Array = std::static_pointer_cast<arrow::Int32Array>(col);
            for(int i = 0 ; i < col->length() ; i++)
                this->partitionToPositionLinks[pid].get()[i] = int32Array->Value(i);
        }

    }


    //------------------------------------------------------------------------------------------//



    vector<shared_ptr<DataPage>> tables_TransformToRecordBatch()
    {
        vector<shared_ptr<DataPage>> pages;
        for(auto partition : tables)
        {
            int partitionId = partition.first;
            auto table = partition.second;
            auto fields = table->schema()->fields();
            vector<string> colNames;
            for(auto field : fields)
                colNames.push_back(field->name());
            for(auto cn : colNames)
            {
                cn.append("_HashJoinBuildComponentsTablesPartitionId_"+ to_string(partitionId));
            }
            auto newTable = renameTableSchema(table,colNames);
            auto recordBatch = newTable->CombineChunksToBatch().ValueOrDie();
            pages.push_back(make_shared<DataPage>(recordBatch));
        }

        return pages;
    }


    void recordBatch_ToTables(shared_ptr<DataPage> page)
    {

        auto batch = page->get();
        auto schema = batch->schema();
        auto allFields = schema->fields();

        if(allFields[0]->name().find("HashJoinBuildComponentsTablesPartitionId") == std::string::npos)
            return;


        vector<string> trueFields;
        int pId = 0;
        for(int i = 0 ; i < allFields.size(); i++)
        {
            vector<string> res;
            StringUtils::Stringsplit(allFields[i]->name(),'_',res);
            string partitionId = res.back();
            string trueFieldName = res[0];
            pId = atoi(partitionId.c_str());

            trueFields.push_back(trueFieldName);
        }
        auto table = arrow::Table::FromRecordBatches({batch});
        auto newTable = renameTableSchema(table.ValueOrDie(),trueFields);

        this->tables[pId] = newTable;
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
            new_fields.push_back(old_fields[i]->WithName(new_names[i]));
        }

        auto new_schema = arrow::schema(new_fields);

        // 创建新 Table（数据不变）
        auto new_table = arrow::Table::Make(new_schema, table->columns());
        return new_table;
    }

    vector<shared_ptr<DataPage>> ToDataPages()
    {
        vector<shared_ptr<DataPage>> pages;

        auto re = partitionToHashKeyArray_TransformToRecordBatch();
        pages.push_back(re);

        re = partitionToPositionLinks_TransformToRecordBatch();
        pages.push_back(re);

        re = partitionToPositionToHashes_TransformToRecordBatch();
        pages.push_back(re);

        vector<shared_ptr<DataPage>> res;
        res = tables_TransformToRecordBatch();
        for(auto re : res)
            pages.push_back(re);

        return pages;
    }


    void fromDataPages(vector<shared_ptr<DataPage>> pages)
    {
        for(auto page : pages)
        {
            recordBatch_ToTables(page);
            recordBatch_ToPartitionToPositionLinks(page);
            recordBatch_ToPartitionToHashKeyArray(page);
            recordBatch_ToPartitionToPositionToHashes(page);
        }
    }

};


#endif //OLVP_HASHJOINBUILDCOMPONENTS_HPP
