//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_ASTCLEANER_HPP
#define FRONTEND_ASTCLEANER_HPP





#include "AstNodes/AstNodeVisitor.h"
#include "AstNodes/tree.h"
#include "spdlog/spdlog.h"
class AstNodeCleaner:public AstNodeVisitor
{

public:


    void* VisitGroupBy(GroupBy* node,void *context) {


        for(auto child : node->getChildren())
            this->Visit(child,context);

        delete(node);
        return NULL;
    }
    void* VisitGroupingElement(GroupingElement* node,void *context) {


        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void* VisitSimpleGroupBy(SimpleGroupBy* node,void *context) {


        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitIsDistinct(IsDistinct* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitOffset(Offset* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);


        return NULL;
    }
    void* VisitOrderBy(OrderBy* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitQuery(Query* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitQuerySpecification(QuerySpecification* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void* VisitSortItem(SortItem* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void* VisitExpression(Expression* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitJoin(Join* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitLateral(Lateral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitQueryBody(QueryBody* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitRelation(Relation* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void* VisitTable(Table* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitFunctionCall(FunctionCall* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitIdentifier(Identifier* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitLogicalBinaryExpression(LogicalBinaryExpression* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }


    void* VisitLiteral(Literal* node,void *context){


        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitStringLiteral(StringLiteral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitDate32Literal(Date32Literal* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitDoubleLiteral(DoubleLiteral* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitInt32Literal(Int32Literal* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitInt64Literal(Int64Literal* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void* VisitSelectItem(SelectItem * node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitSelect(Select* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void* VisitAllColumns(AllColumns* node,void *context) {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitSingleColumn(SingleColumn* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitArithmeticBinaryExpression(ArithmeticBinaryExpression* node,void *context){

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void* VisitComparisionExpression(ComparisionExpression* node,void *context)
    {

        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }
    void* VisitNotExpression(NotExpression* node,void *context){


        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }



    void *VisitCast(Cast *node, void *context) override {

        this->Visit(node->getExpression(), context);
        delete(node);
        return NULL;
    }

    void *VisitSymbolReference(SymbolReference *node, void *context) override {
        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void *VisitFieldReference(FieldReference *node, void *context) override {
        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void * VisitInExpression(InExpression *node, void *context) override
    {
        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void * VisitIfExpression(IfExpression *node, void *context) override
    {
        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }

    void * VisitColumn(Column *node, void *context) override
    {
        for(auto child : node->getChildren())
            this->Visit(child,context);
        delete(node);
        return NULL;
    }




};




#endif //FRONTEND_ASTCLEANER_HPP
