//
// Created by zxk on 5/21/23.
//

#ifndef OLVP_AGGREGATIONDESCRIPTOR_HPP
#define OLVP_AGGREGATIONDESCRIPTOR_HPP

#include <vector>
#include "AggregateDescriptor.hpp"


class AggregationDesc
{
    vector<AggregateDesc> aggregates;
    vector<string> groupByKeys;
public:
    AggregationDesc(){}
    AggregationDesc(vector<AggregateDesc>aggregates,vector<string>groupByKeys){
        this->aggregates = aggregates;
        this->groupByKeys = groupByKeys;
    }

    vector<AggregateDesc> getAggregates()
    {
        return this->aggregates;
    }
    vector<string> getGroupByKeys()
    {
        return this->groupByKeys;
    }


    static string Serialize(AggregationDesc aggregationDesc)
    {
        nlohmann::json agglist = nlohmann::json::array();

        for(auto agg : aggregationDesc.aggregates)
        {
            agglist.push_back(AggregateDesc::Serialize(agg));
        }

        nlohmann::json desc;
        desc["aggregates"] = agglist;
        desc["groupByKeys"] = aggregationDesc.groupByKeys;

        string result = desc.dump();

        return result;
    }
    static AggregationDesc Deserialize(string desc)
    {
        nlohmann::json aggregationDesc = nlohmann::json::parse(desc);

        vector<AggregateDesc> aggs;
        for( auto agg : aggregationDesc["aggregates"])
        {
            AggregateDesc aggregateDesc = AggregateDesc::Deserialize(agg);
            aggs.push_back(aggregateDesc);
        }

        vector<string> groupByKeys = aggregationDesc["groupByKeys"];
        return AggregationDesc(aggs,groupByKeys);
    }


    void test()
    {
        AggregationDesc desc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"s_suppkey",/*outputName=*/"s_nationkey_all"),
                              AggregateDesc(/*functionName=*/"count",/*inputKey=*/"s_suppkey",/*outputName=*/"s_nationkey_count")
                             },
                /*groupByKeys=*/{"s_supppkey","s_name"});


        string descStr = AggregationDesc::Serialize(desc);

        cout << descStr << endl;

        AggregationDesc aggregationDesc = AggregationDesc::Deserialize(descStr);

    }

    string to_string()
    {
        string result;

        for(int i = 0 ; i < this->aggregates.size() ; i++) {
            result += ("aggregate:"+this->aggregates[i].to_string());
            result += "\n";
        }
        result+="groupByKeys:";
        for(int i = 0 ; i < this->groupByKeys.size() ; i++)
        {
            result += (this->groupByKeys[i] +"|");
        }
        result+="\n";

        return result;
    }


};

#endif //OLVP_AGGREGATIONDESCRIPTOR_HPP
