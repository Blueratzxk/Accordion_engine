//
// Created by zxk on 11/1/24.
//

#ifndef FRONTEND_ASTNODEALLOCATOR_HPP
#define FRONTEND_ASTNODEALLOCATOR_HPP
#include "AstNodeVisitor.h"
#include "Relational/Relation.h"
#include "GroupingElement.h"
#include "SimpleGroupBy.hpp"
#include "GroupBy.h"
#include "IsDistinct.h"
#include "Offset.h"
#include "OrderBy.h"
#include "SortItem.h"
#include "Select.h"
#include "SelectItem.h"
#include "AllColumns.h"
#include "SingleColumn.h"
#include "QuerySpecification.h"
#include "Query.h"

#include "Expression/LogicalBinaryExpression.h"
#include "Expression/Identifier.h"
#include "Expression/FunctionCall.h"
#include "Expression/ArithmeticBinaryExpression.h"
#include "Expression/ComparisonExpression.h"
#include "Expression/NotExpression.h"
#include "Expression/FieldReference.hpp"
#include "Expression/SymbolReference.hpp"
#include "Expression/Cast.hpp"

#include "Expression/Literals/Literals.h"
#include "Relational/Join.h"
#include "Relational/Lateral.h"
#include "Relational/QueryBody.h"


#include "Relational/Table.h"
#include "../Analyzer/Type.hpp"


#include <set>
class AstNodeAllocator
{
    vector<Node*> nodes;
public:
    AstNodeAllocator()
    {

    }

    Identifier *new_Identifier(string location, string value, bool hasDelimiter=false)
    {
        auto node = new Identifier(location,value,hasDelimiter);

        nodes.push_back(node);
        return node;
    }

    ArithmeticBinaryExpression* new_ArithmeticBinaryExpression(string location,string opIn,Expression *left,Expression *right)
    {
        auto node = new ArithmeticBinaryExpression(location,opIn,left,right);

        nodes.push_back(node);
        return node;
    }

    GroupBy* new_GroupBy(string location, bool isDistinct,list<GroupingElement*> groupingElements)
    {
        auto node = new GroupBy(location,isDistinct,groupingElements);

        nodes.push_back(node);
        return node;
    }
    GroupingElement* new_GroupingElement(string location,string groupingElementName)
    {
        auto node = new GroupingElement(location,groupingElementName);

        nodes.push_back(node);
        return node;
    }

    IsDistinct* new_IsDistinct(string location)
    {
        auto node = new IsDistinct(location);

        nodes.push_back(node);
        return node;
    }

    Offset* new_Offset(string location)
    {
        auto node = new Offset(location);

        nodes.push_back(node);
        return node;
    }

    OrderBy* new_OrderBy(string location, list<SortItem*> sortItems)
    {
        auto node = new OrderBy(location,sortItems);

        nodes.push_back(node);
        return node;
    }

    Query* new_Query(string location, QueryBody *queryBody)
    {
        auto node = new Query(location,queryBody);

        nodes.push_back(node);
        return node;
    }

    QuerySpecification* new_QuerySpecification(string location, Select *select, Relation *from,Expression *where,GroupBy*groupBy,Expression *having,OrderBy *orderBy,Offset *offset,string limit)
    {
        auto node = new QuerySpecification(location,select,from,where,groupBy,having,orderBy,offset,limit);

        nodes.push_back(node);
        return node;
    }

    SortItem* new_SortItem(string location ,SortItem::Ordering ordering, Expression * sortKey)
    {
        auto node = new SortItem(location,ordering,sortKey);

        nodes.push_back(node);
        return node;
    }

    Expression* new_Expression(string location,string ExpressionId)
    {
        auto node = new Expression(location,ExpressionId);

        nodes.push_back(node);
        return node;
    }

    Join* new_Join(string location, Join::Type type,Relation *left, Relation *right)
    {
        auto node = new Join(location,type,left,right);

        nodes.push_back(node);
        return node;
    }
    Join* new_Join(string location, Join::Type type,Relation *left, Relation *right, shared_ptr<JoinCriteria> joinCriteria)
    {
        auto node = new Join(location,type,left,right,joinCriteria);

        nodes.push_back(node);
        return node;
    }


    Lateral* new_Lateral(string location, Query *query)
    {
        auto node = new Lateral(location,query);

        nodes.push_back(node);
        return node;
    }

    QueryBody* new_QueryBody(string location,string queryBodyId)
    {
        auto node = new QueryBody(location,queryBodyId);

        nodes.push_back(node);
        return node;
    }

    Relation* new_Relation(string location,string relationId)
    {
        auto node = new Relation(location,relationId);

        nodes.push_back(node);
        return node;
    }

    Table* new_Table(string location, string tableName)
    {
        auto node = new Table(location,tableName);

        nodes.push_back(node);
        return node;
    }

    FunctionCall* new_FunctionCall(string location, string functionName,vector<Expression *> arguments)
    {
        auto node = new FunctionCall(location,functionName,arguments);

        nodes.push_back(node);
        return node;
    }

    LogicalBinaryExpression* new_LogicalBinaryExpression(string location, string op,Expression *left,Expression *right)
    {
        auto node = new LogicalBinaryExpression(location,op,left,right);

        nodes.push_back(node);
        return node;
    }

    Literal* new_Literal(string location,string literalId)
    {
        auto node = new Literal(location,literalId);

        nodes.push_back(node);
        return node;
    }

    StringLiteral* new_StringLiteral(string location,string value)
    {
        auto node = new StringLiteral(location,value);

        nodes.push_back(node);
        return node;
    }

    Date32Literal* new_Date32Literal(string location,string value)
    {
        auto node = new Date32Literal(location,value);

        nodes.push_back(node);
        return node;
    }

    DayTimeIntervalLiteral* new_DayTimeIntervalLiteral(string location,int32_t value)
    {
        auto node = new DayTimeIntervalLiteral(location,value);

        nodes.push_back(node);
        return node;
    }

    DoubleLiteral* new_DoubleLiteral(string location,string value)
    {
        auto node = new DoubleLiteral(location,value);

        nodes.push_back(node);
        return node;
    }

    Int32Literal* new_Int32Literal(string location,string value)
    {
        auto node = new Int32Literal(location,value);

        nodes.push_back(node);
        return node;
    }

    Int64Literal* new_Int64Literal(string location,string value)
    {
        auto node = new Int64Literal(location,value);

        nodes.push_back(node);
        return node;
    }

    SelectItem*  new_SelectItem(string location,string selectItemId)
    {
        auto node = new SelectItem(location,selectItemId);

        nodes.push_back(node);
        return node;
    }

    Select* new_Select(string location,list<SelectItem *> selectItems)
    {
        auto node = new Select(location,selectItems);

        nodes.push_back(node);
        return node;
    }

    AllColumns* new_AllColumns(string location)
    {
        auto node = new AllColumns(location);

        nodes.push_back(node);
        return node;
    }

    SingleColumn* new_SingleColumn(string location,Expression *expression)
    {
        auto node = new SingleColumn(location,expression);

        nodes.push_back(node);
        return node;
    }

    NotExpression* new_NotExpression(string location,Expression *expression)
    {
        auto node = new NotExpression(location,expression);

        nodes.push_back(node);
        return node;
    }

    ComparisionExpression* new_ComparisionExpression(string location,string opIn,Expression *left,Expression *right)
    {
        auto node = new ComparisionExpression(location,opIn,left,right);

        nodes.push_back(node);
        return node;
    }

    FieldReference* new_FieldReference(string location, int fieldIndex)
    {
        auto node = new FieldReference(location,fieldIndex);

        nodes.push_back(node);
        return node;
    }

    SymbolReference* new_SymbolReference(string location, string name)
    {
        auto node = new SymbolReference(location,name);

        nodes.push_back(node);
        return node;
    }

    Cast* new_Cast(string location, Expression *expression, string type)
    {
        auto node = new Cast(location,expression,type);

        nodes.push_back(node);
        return node;
    }

    SimpleGroupBy* new_SimpleGroupBy(string location, list<Expression *> columns)
    {
        auto node = new SimpleGroupBy(location,columns);

        nodes.push_back(node);
        return node;
    }

    Node* record(Node *node)
    {
        nodes.push_back(node);
        return node;
    }

    void release_all(){

        for(auto node : this->nodes)
            delete node;
    }
    



};
#endif //FRONTEND_ASTNODEALLOCATOR_HPP
