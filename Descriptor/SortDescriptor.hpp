//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_SORTDESCRIPTOR_HPP
#define OLVP_SORTDESCRIPTOR_HPP

#include "../Operators/SortOperator.hpp"
class SortDescriptor
{
    vector<string> sortKeys;
    vector<SortOperator::SortOrder> sortOrders;

public:
    SortDescriptor(){}

    SortDescriptor(vector<string> sortKeys,vector<SortOperator::SortOrder> sortOrders)
    {
        this->sortKeys = sortKeys;
        this->sortOrders = sortOrders;
    }
    vector<string> getSortKeys()
    {
        return this->sortKeys;
    }
    vector<SortOperator::SortOrder> getSortOrders()
    {
        return this->sortOrders;
    }



    static string Serialize(SortDescriptor sortDescriptor)
    {
        nlohmann::json desc;


        desc["SortKeys"] = sortDescriptor.sortKeys;
        desc["SortOrders"] = sortDescriptor.sortOrders;

        string result = desc.dump();

        return result;
    }
    static SortDescriptor Deserialize(string desc)
    {
        nlohmann::json sortDesc = nlohmann::json::parse(desc);
        return SortDescriptor(sortDesc["SortKeys"],sortDesc["SortOrders"]);
    }


    void test()
    {

        vector<string> sortKeys = {"s_nationkey"};
        vector<SortOperator::SortOrder> sortOrders = {SortOperator::Descending};

        SortDescriptor desc(sortKeys,sortOrders);

        string sort = SortDescriptor::Serialize(desc);

        cout << sort << endl;

        SortDescriptor descriptor = SortDescriptor::Deserialize(sort);

        cout << SortDescriptor::Serialize(descriptor) << endl;


        cout << "----------------------------------"<<endl;

    }

};


#endif //OLVP_SORTDESCRIPTOR_HPP
