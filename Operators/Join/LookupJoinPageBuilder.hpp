//
// Created by zxk on 5/25/23.
//

#ifndef OLVP_LOOKUPJOINPAGEBUILDER_HPP
#define OLVP_LOOKUPJOINPAGEBUILDER_HPP

#include "arrow/array.h"
#include "../../Page/DataPageBuilder.hpp"
#include "JoinProbe.hpp"
#include "LookupSource.hpp"
class LookupJoinPageBuilder
{
    std::shared_ptr<arrow::Int32Builder> probeIndexBuilder;
    std::shared_ptr<DataPageBuilder> buildPageBuilder;
    int buildOutputChannelCount;
    int previousPosition = -1;

    std::shared_ptr<arrow::Schema> buildOutputSchema;

    int probeRowBytes = -1;

public:
    LookupJoinPageBuilder(std::shared_ptr<arrow::Schema> buildOutputSchema){

        this->buildPageBuilder = std::make_shared<DataPageBuilder>(buildOutputSchema);
        this->buildOutputChannelCount = buildOutputSchema->num_fields();
        this->buildOutputSchema = buildOutputSchema;
        this->probeIndexBuilder = std::make_shared<arrow::Int32Builder>();

    }

    bool isFull()
    {

        return this->probeRowBytes*this->probeIndexBuilder->length() + this->buildPageBuilder->getSizeInBytes() > 4*1024*1024 ;
    }

    bool isEmpty()
    {
        return false;
    }

    void ResetStatus()
    {
        this->probeIndexBuilder->Reset();
        this->buildPageBuilder->Reset();
    }

    void appendRow(std::shared_ptr<JoinProbe> probe, std::shared_ptr<LookupSource> lookupSource, long joinPosition)
    {
        // probe side
        appendProbeIndex(probe);

        // build side
        buildPageBuilder->declarePosition();
        lookupSource->appendTo(joinPosition, buildPageBuilder, 0);
    }



    void appendProbeIndex(std::shared_ptr<JoinProbe> probe)
    {
        int position = probe->getProbePosition();
        arrow::Status status = probeIndexBuilder->Append(position);
        if(!status.ok())
        {
            spdlog::critical("LookupJoinPageBuilder appendProbeIndex ERROE!");
        }


        if(this->probeRowBytes == -1) {
            vector<int> outputs = probe->getOutputChannels();
            std::shared_ptr<DataPage> page = probe->getPage();
            int allBytes = 0;
            for (int i = 0; i < outputs.size(); i++) {
                allBytes += page->get()->schema()->field(outputs[i])->type()->byte_width();
            }
            this->probeRowBytes = allBytes;
        }

    }

    std::shared_ptr<DataPage> build(std::shared_ptr<JoinProbe> probe)
    {
        vector<std::shared_ptr<arrow::Array>> outputArrays;

        int outputPositions = this->probeIndexBuilder->length();
        vector<int> probeOutputChannels = probe->getOutputChannels();

        int outputChannelsCount = probeOutputChannels.size()+this->buildOutputChannelCount;

        std::shared_ptr<DataPage> probePage = probe->getPage();

        std::shared_ptr<arrow::Int32Array> indexs;
        arrow::Status status = this->probeIndexBuilder->Finish(&indexs);

        if(!status.ok()) {
            spdlog::critical("LookupJoinPageBuilder probeIndexBuilder ERROE!");
            return NULL;
        }


        for(int i = 0 ; i < probeOutputChannels.size() ; i++) {
            outputArrays.push_back(probePage->getSlice(probeOutputChannels[i],indexs));
        }

        int offset = probeOutputChannels.size();

        for(int i = 0 ; i < this->buildOutputChannelCount ; i++)
        {
            outputArrays.push_back(buildPageBuilder->getArrayBuilder(i)->buildFinish().ValueOrDie());
        }

        vector<std::shared_ptr<arrow::Field>> fields;


        for(int i = 0 ; i < probeOutputChannels.size() ; i++) {
            fields.push_back(probe->getPage()->get()->schema()->field(probeOutputChannels[i]));
        }
        for(int i = 0 ; i < this->buildOutputSchema->num_fields() ; i++)
            fields.push_back(this->buildOutputSchema->field(i));

        auto allSchema = std::make_shared<arrow::Schema>(fields);

        auto batch = arrow::RecordBatch::Make(allSchema,outputArrays[0]->length(),outputArrays);

        return std::make_shared<DataPage>(batch);

    }

};

#endif //OLVP_LOOKUPJOINPAGEBUILDER_HPP
