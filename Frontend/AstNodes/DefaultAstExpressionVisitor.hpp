//
// Created by zxk on 10/22/24.
//

#ifndef FRONTEND_DEFAULTASTEXPRESSIONVISITOR_HPP
#define FRONTEND_DEFAULTASTEXPRESSIONVISITOR_HPP





#include "AstNodeVisitor.h"
#include "tree.h"

class DefaultAstExpressionVisitor:public AstNodeVisitor{

public:


    void* VisitGroupBy(GroupBy* node,void *context)override {

        return Visit(node,context);
    }
    void* VisitGroupingElement(GroupingElement* node,void *context) override{


        return Visit(node,context);
    }

    void* VisitSimpleGroupBy(SimpleGroupBy* node,void *context) override{


        return Visit(node,context);
    }
    void* VisitIsDistinct(IsDistinct* node,void *context)override{

        return Visit(node,context);
    }
    void* VisitOffset(Offset* node,void *context) override{

        return Visit(node,context);
    }
    void* VisitOrderBy(OrderBy* node,void *context) override{

        return Visit(node,context);
    }
    void* VisitQuery(Query* node,void *context) override{

        return Visit(node,context);
    }
    void* VisitQuerySpecification(QuerySpecification* node,void *context) override{

        return VisitQueryBody(node,context);

    }

    void* VisitSortItem(SortItem* node,void *context) override{

        return Visit(node,context);

    }

    void* VisitExpression(Expression* node,void *context)  override{

        return Visit(node,context);

    }
    void* VisitJoin(Join* node,void *context) override{

        return VisitRelation(node,context);
    }
    void* VisitLateral(Lateral* node,void *context) override{

        return VisitRelation(node,context);
    }
    void* VisitQueryBody(QueryBody* node,void *context) override {

        return VisitRelation(node,context);
    }
    void* VisitRelation(Relation* node,void *context)  override{

        return Visit(node,context);

    }

    void* VisitTable(Table* node,void *context) override{

        return VisitQueryBody(node,context);
    }
    void* VisitFunctionCall(FunctionCall* node,void *context) override {

        return VisitExpression(node,context);
    }
    void* VisitIdentifier(Identifier* node,void *context) override{

        return VisitExpression(node,context);
    }
    void* VisitLogicalBinaryExpression(LogicalBinaryExpression* node,void *context) override{

        return VisitExpression(node,context);
    }


    void* VisitLiteral(Literal* node,void *context) override{


        return VisitExpression(node,context);
    }
    void* VisitStringLiteral(StringLiteral* node,void *context) override{

        return VisitLiteral(node,context);
    }
    void* VisitDate32Literal(Date32Literal* node,void *context) override{

        return VisitLiteral(node,context);
    }
    void* VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral* node,void *context) override{

        return VisitLiteral(node,context);
    }
    void* VisitDoubleLiteral(DoubleLiteral* node,void *context) override{

        return VisitLiteral(node,context);
    }
    void* VisitInt32Literal(Int32Literal* node,void *context) override{

        return VisitLiteral(node,context);
    }
    void* VisitInt64Literal(Int64Literal* node,void *context) override{

        return VisitLiteral(node,context);
    }

    void* VisitSelectItem(SelectItem * node,void *context) override{

        return Visit(node,context);
    }
    void* VisitSelect(Select* node,void *context) override{

        return Visit(node,context);
    }

    void* VisitAllColumns(AllColumns* node,void *context)  override{

        return VisitSelectItem(node,context);
    }
    void* VisitSingleColumn(SingleColumn* node,void *context) override{

        return VisitSelectItem(node,context);
    }
    void* VisitArithmeticBinaryExpression(ArithmeticBinaryExpression* node,void *context) override{

        return VisitExpression(node,context);
    }
    void* VisitComparisionExpression(ComparisionExpression* node,void *context) override
    {

        return VisitExpression(node,context);
    }

    void* VisitNotExpression(NotExpression* node,void *context) override{

        return VisitExpression(node,context);
    }


    void *VisitCast(Cast *node, void *context) override {

        return VisitExpression(node,context);
    }

    void *VisitSymbolReference(SymbolReference *node, void *context) override {

        return VisitExpression(node,context);
    }

    void *VisitFieldReference(FieldReference *node, void *context) override {

        return VisitExpression(node,context);
    }

    void * VisitColumn(Column *node, void *context) override
    {
        return VisitExpression(node,context);
    }
    void * VisitIfExpression(IfExpression *node, void *context) override
    {
        return VisitExpression(node,context);
    }

    void * VisitInExpression(InExpression *node, void *context) override
    {
        return VisitExpression(node,context);
    }


};




#endif //FRONTEND_DEFAULTASTEXPRESSIONVISITOR_HPP
