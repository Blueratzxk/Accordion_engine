//
// Created by zxk on 10/2/24.
//

#ifndef FRONTEND_ASTVIEWER_HPP
#define FRONTEND_ASTVIEWER_HPP



#include "AstNodes/AstNodeVisitor.h"
#include "AstNodes/tree.h"
#include "spdlog/spdlog.h"
class AstNodeViewer:public AstNodeVisitor {


public:


    void *VisitGroupBy(GroupBy *node, void *context) {

        //    spdlog::info("Node:GroupBy");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitSimpleGroupBy(SimpleGroupBy *node, void *context) {
           spdlog::info("SimpleGroupBy");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }


    void *VisitGroupingElement(GroupingElement *node, void *context) {

        spdlog::info(node->getGroupingElementName());

        return NULL;
    }

    void *VisitIsDistinct(IsDistinct *node, void *context) {
        //    spdlog::info("Node:IsDistinct");
        for (auto child: node->getChildren())
            this->Visit(child, context);


        return NULL;
    }

    void *VisitOffset(Offset *node, void *context) {
        //    spdlog::info("Node:Offset");
        for (auto child: node->getChildren())
            this->Visit(child, context);


        return NULL;
    }

    void *VisitOrderBy(OrderBy *node, void *context) {
        //    spdlog::info("Node:OrderBy");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitQuery(Query *node, void *context) {
        //    spdlog::info("Node:Query");
        for (auto child: node->getChildren())
            this->Visit(child, context);


        return NULL;
    }

    void *VisitQuerySpecification(QuerySpecification *node, void *context) {
        //   spdlog::info("Node:QuerySpecification");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;

    }

    void *VisitSortItem(SortItem *node, void *context) {
        //   spdlog::info("Node:SortItem");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;

    }

    void *VisitExpression(Expression *node, void *context) {
        //   spdlog::info("Node:Expression");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;

    }

    void *VisitJoin(Join *node, void *context) {
        //   spdlog::info("Node:Join");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitLateral(Lateral *node, void *context) {
        //   spdlog::info("Node:Lateral");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitQueryBody(QueryBody *node, void *context) {
        //   spdlog::info("Node:QueryBody");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitRelation(Relation *node, void *context) {
        //   spdlog::info("Node:Relation");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitTable(Table *node, void *context) {
        //   spdlog::info("Node:Table");
        spdlog::info(node->getTableName());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitFunctionCall(FunctionCall *node, void *context) {
        //   spdlog::info("Node:FunctionCall");
        spdlog::info(node->getFuncName());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitIdentifier(Identifier *node, void *context) {
        //   spdlog::info("Node:Identifier");
        spdlog::info(node->getValue());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitLogicalBinaryExpression(LogicalBinaryExpression *node, void *context) {
        //   spdlog::info("Node:LogicalBinaryExpression");
        spdlog::info(node->getOp());
        for (auto child: node->getChildren())
            this->Visit(child, context);


        return NULL;
    }


    void *VisitLiteral(Literal *node, void *context) {
        //    spdlog::info("Node:Literal");

        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitStringLiteral(StringLiteral *node, void *context) {
        //  spdlog::info("Node:StringLiteral");
        spdlog::info(node->getValue());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitDate32Literal(Date32Literal *node, void *context) {
        //  spdlog::info("Node:Date32Literal");
        spdlog::info(node->getValue());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitDayTimeIntervalLiteral(DayTimeIntervalLiteral *node, void *context) {
        //  spdlog::info("Node:DayTimeIntervalLiteral");
        spdlog::info(node->getValue());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitDoubleLiteral(DoubleLiteral *node, void *context) {
        //  spdlog::info("Node:DoubleLiteral");
        spdlog::info(node->getValue());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitInt32Literal(Int32Literal *node, void *context) {
        //spdlog::info("Node:Int32Literal");
        spdlog::info(node->getValue());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitInt64Literal(Int64Literal *node, void *context) {
        // spdlog::info("Node:Int64Literal");
        spdlog::info(node->getValue());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitSelectItem(SelectItem *node, void *context) {
        // spdlog::info("Node:SelectItem");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitSelect(Select *node, void *context) {
        //spdlog::info("Node:Select");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitAllColumns(AllColumns *node, void *context) {
        // spdlog::info("Node:AllColumns");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitSingleColumn(SingleColumn *node, void *context) {
        //spdlog::info("Node:SingleColumn");
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitArithmeticBinaryExpression(ArithmeticBinaryExpression *node, void *context) {
        // spdlog::info("Node:ArithmeticBinaryExpression");
        spdlog::info(node->getOp());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitComparisionExpression(ComparisionExpression *node, void *context) {
        spdlog::info(node->getOp());
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void *VisitNotExpression(NotExpression *node, void *context) {
        //spdlog::info("Node:NotExpression");
        spdlog::info(node->getOp());

        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }


    void *VisitCast(Cast *node, void *context) override {
        spdlog::info("Cast");
        this->Visit(node->getExpression(), context);
        return NULL;
    }

    void *VisitSymbolReference(SymbolReference *node, void *context) override {
        spdlog::info(node->getName());
        return NULL;
    }

    void *VisitFieldReference(FieldReference *node, void *context) override {
        spdlog::info(node->getFieldIndex());
        return NULL;
    }

    void * VisitInExpression(InExpression *node, void *context) override
    {

        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void * VisitIfExpression(IfExpression *node, void *context) override
    {
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }

    void * VisitColumn(Column *node, void *context) override
    {
        for (auto child: node->getChildren())
            this->Visit(child, context);

        return NULL;
    }


};




#endif //FRONTEND_ASTVIEWER_HPP
