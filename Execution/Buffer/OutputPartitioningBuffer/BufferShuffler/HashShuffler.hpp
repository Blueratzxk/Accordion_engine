//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_HASHSHUFFLER_HPP
#define OLVP_HASHSHUFFLER_HPP


//#include "../../OutputBufferSchema.hpp"
#include "SimplePageHashGenerator.hpp"
#include "../../../../Page/DataPageBuilder.hpp"
#include <atomic>

class HashShuffler
{
    shared_ptr<SimplePageHashGenerator> hashGen;
    map<int,shared_ptr<DataPageBuilder>> partitionedPage;

public:

    HashShuffler(vector<int> hashChannelIndexs)
    {
        this->hashGen = make_shared<SimplePageHashGenerator>(hashChannelIndexs);
    }

    inline void processOptimize(int partitionCount,shared_ptr<DataPage> page)
    {
        map<int,vector<int>> partitionAssignment;


        for(int i = 0 ; i < page->getElementsCount() ; i++) {
            int partitionNumber = this->hashGen->getPartition(partitionCount, i, page);
            partitionAssignment[partitionNumber].push_back(i);
        }

        for(auto partition : partitionAssignment)
        {
            partitionedPage[partition.first] = make_shared<DataPageBuilder>(page->get()->schema());
            partitionedPage[partition.first]->appendRows(partition.second,page);
        }


    }
    inline void process(int partitionCount,shared_ptr<DataPage> page)
    {
        for(int i = 0 ; i < page->getElementsCount() ; i++)
        {
            int partitionNumber = hashGen->getPartition(partitionCount,i,page);

            if(partitionedPage.count(partitionNumber) == 0)
            {
                partitionedPage[partitionNumber] = make_shared<DataPageBuilder>(page->get()->schema());
                partitionedPage[partitionNumber]->appendRow(i,page);
            }
            else
                partitionedPage[partitionNumber]->appendRow(i,page);

        }
    }
    inline vector<shared_ptr<DataPage>> build()
    {
        vector<shared_ptr<DataPage>> pages;
        for(auto page : this->partitionedPage)
        {
            pages.push_back(page.second->build());
            page.second = NULL;
        }
        this->partitionedPage.clear();
        return pages;
    }
    inline map<int,shared_ptr<DataPage>> buildMap()
    {
        map<int,shared_ptr<DataPage>> resultMap;

        for(auto page : this->partitionedPage)
        {
            resultMap[page.first] = (page.second->build());
   //         spdlog::info(page.second->build()->get()->ToString());
            page.second = NULL;



        }
        //for(auto page : resultMap)
      //  {
       //     spdlog::info(page.second->get()->ToString());
       // }

        this->partitionedPage.clear();
        return resultMap;
    }

    inline map<int,shared_ptr<DataPageBuilder>> getPartitionedPage()
    {
        return this->partitionedPage;
    }





};


#endif //OLVP_HASHSHUFFLER_HPP
