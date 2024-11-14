//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_TOPKDESCRIPTOR_HPP
#define OLVP_TOPKDESCRIPTOR_HPP

#include "../Operators/TopKOperator.hpp"

using namespace std;
class TopKDescriptor
{
    int K;
    vector<string> sortKeysTopk;
    vector<TopKOperator::SortOrder> sortOrdersTopK;

public:
    TopKDescriptor(){}
    TopKDescriptor(int K,vector<string> sortKeysTopk,vector<TopKOperator::SortOrder> sortOrdersTopK){
        this->K = K;
        this->sortKeysTopk = sortKeysTopk;
        this->sortOrdersTopK = sortOrdersTopK;
    }


    int getK(){
        return this->K;
    }
    vector<string> getSortKeysTopk()
    {
        return this->sortKeysTopk;
    }
    vector<TopKOperator::SortOrder> getSortOrdersTopKey()
    {
        return this->sortOrdersTopK;
    }



    static string Serialize(TopKDescriptor topKDescriptor)
    {
        nlohmann::json desc;

        desc["K"] = topKDescriptor.K;
        desc["sortKeysTopk"] = topKDescriptor.sortKeysTopk;
        desc["sortOrdersTopK"] = topKDescriptor.sortOrdersTopK;


        string result = desc.dump();

        return result;
    }
    static TopKDescriptor Deserialize(string desc)
    {
        nlohmann::json topkDescriptor = nlohmann::json::parse(desc);

        return TopKDescriptor(topkDescriptor["K"], topkDescriptor["sortKeysTopk"],topkDescriptor["sortOrdersTopK"]);
    }


    void test()
    {


        vector<string> sortKeysTopk = {"s_acctbal"};
        vector<TopKOperator::SortOrder> sortOrdersTopK = {TopKOperator::Descending};

        TopKDescriptor td(10,sortKeysTopk,sortOrdersTopK);

        string tds = TopKDescriptor::Serialize(td);

        cout << tds << endl;

        TopKDescriptor d = TopKDescriptor::Deserialize(tds);

        cout << TopKDescriptor::Serialize(d) << endl;

        cout << "----------------------------------"<<endl;


    }




};
#endif //OLVP_TOPKDESCRIPTOR_HPP
