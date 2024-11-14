//
// Created by zxk on 10/1/24.
//

#ifndef FRONTEND_ASTNODEVISITOR_H
#define FRONTEND_ASTNODEVISITOR_H



#include "Node.h"

class GroupBy;
class GroupingElement;
class IsDistinct;
class Offset;
class OrderBy;
class Query;
class QuerySpecification;
class SortItem;
class Statement;
class Expression;
class Join;
class Lateral;
class QueryBody;
class Relation;
class Table;
class FunctionCall;
class Identifier;
class LogicalBinaryExpression;
class Literal;
class StringLiteral;
class SimpleGroupBy;


class StringLiteral;
class Date32Literal;
class DayTimeIntervalLiteral;
class DoubleLiteral;
class Int32Literal;
class Int64Literal;
class SelectItem;
class Select;
class AllColumns;
class SingleColumn;
class NotExpression;
class ArithmeticBinaryExpression;
class ComparisionExpression;
class FieldReference;
class SymbolReference;
class Cast;

class Column;
class IfExpression;
class InExpression;

class AstNodeVisitor {

public:
    //virtual ~NodeVisitor();


    virtual void* Visit(Node* node,void *context) {

        return node->accept(this,context);

    }
    virtual void* VisitGroupBy(GroupBy* node,void *context) = 0;
    virtual void* VisitGroupingElement(GroupingElement* node,void *context) = 0;
    virtual void* VisitIsDistinct(IsDistinct* node,void *context) = 0;
    virtual void* VisitOffset(Offset* node,void *context) = 0;
    virtual void* VisitOrderBy(OrderBy* node,void *context) = 0;
    virtual void* VisitQuery(Query* node,void *context) = 0;
    virtual void* VisitQuerySpecification(QuerySpecification* node,void *context) = 0;
    virtual void* VisitSortItem(SortItem* node,void *context) = 0;

    virtual void* VisitExpression(Expression* node,void *context) = 0;
    virtual void* VisitJoin(Join* node,void *context) = 0;
    virtual void* VisitLateral(Lateral* node,void *context) = 0;
    virtual void* VisitQueryBody(QueryBody* node,void *context) = 0;
    virtual void* VisitRelation(Relation* node,void *context) = 0;
    virtual void* VisitTable(Table* node,void *context) = 0;
    virtual void* VisitFunctionCall(FunctionCall* node,void *context) = 0;
    virtual void* VisitIdentifier(Identifier* node,void *context) = 0;
    virtual void* VisitLogicalBinaryExpression(LogicalBinaryExpression* node,void *context) = 0;


    virtual void* VisitLiteral(Literal* node,void *context) = 0;
    virtual void* VisitStringLiteral(StringLiteral* node,void *context) = 0;
    virtual void* VisitDate32Literal(Date32Literal* node,void *context) = 0;
    virtual void* VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral* node,void *context) = 0;
    virtual void* VisitDoubleLiteral(DoubleLiteral* node,void *context) = 0;
    virtual void* VisitInt32Literal(Int32Literal* node,void *context) = 0;
    virtual void* VisitInt64Literal(Int64Literal* node,void *context) = 0;

    virtual void* VisitSelectItem(SelectItem * node,void *context) = 0;
    virtual void* VisitSelect(Select* node,void *context) = 0;

    virtual void* VisitAllColumns(AllColumns* node,void *context) = 0;
    virtual void* VisitSingleColumn(SingleColumn* node,void *context) = 0;
    virtual void* VisitArithmeticBinaryExpression(ArithmeticBinaryExpression* node,void *context) = 0;
    virtual void* VisitNotExpression(NotExpression* node,void *context) = 0;

    virtual void* VisitComparisionExpression(ComparisionExpression* node,void *context) = 0;
    virtual void* VisitFieldReference(FieldReference* node,void *context) = 0;
    virtual void* VisitSymbolReference(SymbolReference* node,void *context) = 0;
    virtual void* VisitCast(Cast* node,void *context) = 0;
    virtual void* VisitSimpleGroupBy(SimpleGroupBy* node,void *context) = 0;


    virtual void* VisitIfExpression(IfExpression *node,void* context) = 0;
    virtual void* VisitInExpression(InExpression *node,void* context) = 0;
    virtual void* VisitColumn(Column* node,void* context)  = 0 ;


};


#endif //FRONTEND_ASTNODEVISITOR_H
