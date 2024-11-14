//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_DEFAULTASTNODEVISITOR_HPP
#define FRONTEND_DEFAULTASTNODEVISITOR_HPP


#include "AstNodeVisitor.h"
#include "tree.h"

class DefaultAstNodeVisitor:public AstNodeVisitor{

public:

    void* VisitGroupBy(GroupBy* node,void *context) {


        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitGroupingElement(GroupingElement* node,void *context) {



        return NULL;
    }


    void* VisitSimpleGroupBy(SimpleGroupBy* node,void *context) override{

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitIsDistinct(IsDistinct* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);


        return NULL;
    }
    void* VisitOffset(Offset* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);


        return NULL;
    }
    void* VisitOrderBy(OrderBy* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitQuery(Query* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);


        return NULL;
    }
    void* VisitQuerySpecification(QuerySpecification* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;

    }

    void* VisitSortItem(SortItem* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;

    }

    void* VisitExpression(Expression* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;

    }
    void* VisitJoin(Join* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitLateral(Lateral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitQueryBody(QueryBody* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitRelation(Relation* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }

    void* VisitTable(Table* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitFunctionCall(FunctionCall* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitIdentifier(Identifier* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitLogicalBinaryExpression(LogicalBinaryExpression* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);


        return NULL;
    }


    void* VisitLiteral(Literal* node,void *context){


        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitStringLiteral(StringLiteral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitDate32Literal(Date32Literal* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitDoubleLiteral(DoubleLiteral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitInt32Literal(Int32Literal* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitInt64Literal(Int64Literal* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }

    void* VisitSelectItem(SelectItem * node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitSelect(Select* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }

    void* VisitAllColumns(AllColumns* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitSingleColumn(SingleColumn* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitArithmeticBinaryExpression(ArithmeticBinaryExpression* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }
    void* VisitComparisionExpression(ComparisionExpression* node,void *context)
    {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }

    void* VisitNotExpression(NotExpression* node,void *context){


        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }


    void *VisitCast(Cast *node, void *context) override {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }

    void *VisitSymbolReference(SymbolReference *node, void *context) override {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }

    void *VisitFieldReference(FieldReference *node, void *context) override {

        for(auto child : node->getChildren())
            this->Visit(child,context);

        return NULL;
    }

    void * VisitColumn(Column *node, void *context) override
    {
        return NULL;
    }
    void * VisitIfExpression(IfExpression *node, void *context) override
    {
        return NULL;
    }

    void * VisitInExpression(InExpression *node, void *context) override
    {
        return NULL;
    }


};




#endif //FRONTEND_DEFAULTASTNODEVISITOR_HPP
