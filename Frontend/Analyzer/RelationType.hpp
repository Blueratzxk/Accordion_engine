//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_RELATIONTYPE_HPP
#define FRONTEND_RELATIONTYPE_HPP

#include "Field.hpp"
#include <vector>

class RelationType
{

    vector<shared_ptr<Field>> allFields;
    map<shared_ptr<Field>,int> fieldIndexs;

public:
    RelationType(vector<shared_ptr<Field>> allFields)
    {
        this->allFields = allFields;

        for(int i = 0 ; i < allFields.size() ; i++)
            fieldIndexs[allFields[i]] = i;
    }
    vector<shared_ptr<Field>> getAllFields()
    {
        return this->allFields;
    }

    shared_ptr<Field> getFieldByIndex(int fieldIndex)
    {
        return allFields[fieldIndex];
    }

    bool canResolve(string name)
    {
        return !resolveFields(name).empty();
    }

    vector<shared_ptr<Field>> resolveFields(string name)
    {
        vector<shared_ptr<Field>> results;

        for(int i = 0 ; i < this->allFields.size() ; i++)
        {
            if(allFields[i]->getValue() == name)
            {
                results.push_back(allFields[i]);
            }
        }
        return results;
    }

    int getAllFieldCount()
    {
        return this->allFields.size();
    }

    int indexOf(shared_ptr<Field> field)
    {
        return this->fieldIndexs[field];
    }

    RelationType()
    {
    }

   shared_ptr<RelationType> joinWith(shared_ptr<RelationType> other)
    {

        vector<shared_ptr<Field>> all = allFields;
        for(auto field : other->allFields)
            all.push_back(field);

        return make_shared<RelationType>(all);
    }



};
#endif //FRONTEND_RELATIONTYPE_HPP
