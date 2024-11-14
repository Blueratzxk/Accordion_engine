//
// Created by zxk on 5/30/23.
//

#ifndef OLVP_TABLESCANNODE_HPP
#define OLVP_TABLESCANNODE_HPP

#include "../../Utils/UUID.hpp"
#include "../../Descriptor/TableScanDescriptor.hpp"
#include "../Planner/TableHandle.hpp"
class ColumnHandle;
class TableScanNode:public PlanNode
{

    string location;
    string id;
    shared_ptr<TableHandle> tableHandle;
    vector<shared_ptr<VariableReferenceExpression>> outputVariables;
    map<shared_ptr<VariableReferenceExpression>,shared_ptr<ColumnHandle>> columns;

    TableScanDescriptor tableScanDescriptor;


    bool planPhase = false;

public:
    TableScanNode(string id,TableScanDescriptor tableScanDescriptor) :PlanNode("TableScanNode",id)
    {
        this->tableScanDescriptor = tableScanDescriptor;
    }

    TableScanNode(string location, string id, shared_ptr<TableHandle> tableHandle,
                  vector<shared_ptr<VariableReferenceExpression>> outputVariables,
                  map<shared_ptr<VariableReferenceExpression>,shared_ptr<ColumnHandle>> columns) :PlanNode("TableScanNode",id)
    {
        this->location = location;
        this->id = id;
        this->tableHandle = tableHandle;
        this->outputVariables = outputVariables;
        this->columns = columns;
        this->planPhase = true;
    }

    shared_ptr<TableHandle> getTableHandle()
    {
        return this->tableHandle;
    }


    void transform() override
    {
        planPhase = false;
        tableScanDescriptor = TableScanDescriptor(this->tableHandle->getCatalogName(),this->tableHandle->getSchemaName(),this->tableHandle->getTableName());

    }

    string generateId()
    {
        return "tableScan_"+UUID::create_uuid();
    }

    TableScanDescriptor getDescriptor()
    {
        return this->tableScanDescriptor;
    }


    void addSource(PlanNode *node){}
    void addSources(PlanNode *node){}

    void* accept(NodeVisitor* visitor,void* context)  {
        return visitor->VisitTableScanNode(this,context);
    }

    vector<shared_ptr<VariableReferenceExpression>> getOutputVariables() override{
        return this->outputVariables;
    }




    PlanNode* replaceChildren(vector<PlanNode*> newChildren){

        if(!planPhase)
        {
            if(this->getId().compare("-1") == 0)
                return new TableScanNode(generateId(),tableScanDescriptor);
            else
                return new TableScanNode(this->getId(),tableScanDescriptor);
        }

        if(this->getId().compare("-1") == 0)
            return new TableScanNode(location,"4234",tableHandle,outputVariables,columns);
        else
            return new TableScanNode(location,this->getId(),tableHandle,outputVariables,columns);
    }

    string getId()
    {
        return PlanNode::getId();
    }



};
#endif //OLVP_TABLESCANNODE_HPP
