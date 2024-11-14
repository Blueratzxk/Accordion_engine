//
// Created by zxk on 10/3/24.
//

#ifndef FRONTEND_STATEMENTANALYZER_HPP
#define FRONTEND_STATEMENTANALYZER_HPP

#include "../AstNodes/DefaultAstNodeVisitor.hpp"
#include "Scope.hpp"
#include "ExpressionAnalysis.hpp"
#include "ExpressionAnalyzer.hpp"
#include "spdlog/spdlog.h"
#include "FieldReference.hpp"
#include "QualifiedObjectName.hpp"

#include "Analysis.hpp"
#include "../../DataSource/SchemaManager.hpp"
#include "ExceptionCollector.hpp"
#include "../Planner/Expression/ExpressionTreeRewriter.hpp"

#include "../Planner/TableHandle.hpp"
#include "../Planner/TpchColumnHandle.hpp"
#include "AggregationAnalyzer.hpp"
#include "../AstNodes/JoinCriteria/JoinOn.hpp"
#include "../AstCleaner.hpp"

class StatementAnalyzer: public enable_shared_from_this<StatementAnalyzer>
{

    Node *root;
    shared_ptr<Analysis> analysis;
    shared_ptr<CatalogsMetaManager> catalogsMetaManager;
    shared_ptr<ExpressionAnalyzer> expressionAnalyzer;
    shared_ptr<FunctionAndTypeResolver> functionAndTypeResolver;
    shared_ptr<ExceptionCollector> exceptionCollector;
    shared_ptr<AstNodeAllocator> astAllocator;



    class Visitor : public DefaultAstNodeVisitor {

        Scope *outerQueryScope = NULL;
        shared_ptr<StatementAnalyzer> statementAnalyzer = NULL;
    public:
        Visitor(Scope *scope, shared_ptr<StatementAnalyzer> statementAnalyzer) {
            this->outerQueryScope = scope;
            this->statementAnalyzer = statementAnalyzer;
        }

        Visitor(shared_ptr<StatementAnalyzer> statementAnalyzer) {
            this->statementAnalyzer = statementAnalyzer;
        }

        void *VisitQuery(Query *node, void *context) override {


            shared_ptr<ScopeBuilder> scopeBuilder = make_shared<ScopeBuilder>();
            Node *queryBody = node->getQueryBody();

            if(queryBody == NULL)
                return NULL;

            Scope *withScope = createScope("WithScope",(Scope*)context);

            this->statementAnalyzer->analysis->addVoidScope(withScope);

            Scope *queryBodyScope =  (Scope*)Visit(queryBody,withScope);

           // analysis.setOutputExpressions(node, descriptorToFields(queryBodyScope));

            Scope *queryScope = scopeBuilder->withParent(withScope)->withRelationType(make_shared<RelationId>(node),
                    queryBodyScope->getRelationType())
                    ->build("QueryScope");

            statementAnalyzer->analysis->setScope(node, queryScope);
            return queryScope;

        }

        void *VisitQuerySpecification(QuerySpecification *node, void *context) override {


            Scope * sourceScope = analyzeFrom(node, (Scope*)context);


            if (node->getWhere() != NULL) {
                Expression *predicate = node->getWhere();
                analyzeWhere(node, sourceScope, predicate);
            }

            list<Expression*> outputExpressions = analyzeSelect(node, sourceScope);

            list<Expression*> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
            if(groupByExpressions.empty() && !this->statementAnalyzer->analysis->getAggregates(node).empty())
                this->statementAnalyzer->analysis->setGroupByExpressions(node,{});


            if(this->statementAnalyzer->exceptionCollector->getErrors().empty())
                verifyAggregations(node,groupByExpressions,outputExpressions);

            //analyzeHaving(node, sourceScope);


            Scope * outputScope = computeAndAssignOutputScope(node, (Scope*)context, sourceScope);


            return outputScope;

        }

        void verifyAggregations(QuerySpecification *node,list<Expression*> groupByExpressions,
                                list<Expression*> outputExpressions)
        {

            if(this->statementAnalyzer->analysis->getAggregates(node).empty())
                return;

            shared_ptr<AggregationAnalyzer> aggregationAnalyzer = make_shared<AggregationAnalyzer>(groupByExpressions,statementAnalyzer->functionAndTypeResolver,
                                                                                                   statementAnalyzer->analysis,
                                                                                                   statementAnalyzer->exceptionCollector);

            for(auto expression : outputExpressions)
                aggregationAnalyzer->analyze(expression);

        }




        string change(string type)
        {
            int leftBracket = type.find('(');
            string typeString = type.substr(0,leftBracket);
            return typeString;
        }
        void *VisitTable(Table *node, void *context) override {


            shared_ptr<QualifiedObjectName> name = createQualifiedObjectName(node->getTableName());


            vector<shared_ptr<Field>> fields ;

            auto table = statementAnalyzer->catalogsMetaManager->getTable(name->getCatalogName(),name->getSchemaName(),name->getObjectName());

            if(TableInfo::isEmpty(table))
            {

                this->statementAnalyzer->exceptionCollector->recordError("Cannot find the table "+node->getTableName()+"!");

                return createAndAssignScope("TableScope",node, (Scope*)context, fields);
            }


            for(int i = 0 ; i < table.getColumnNames().size() ; i++)
            {
                string columnName = table.getColumnNames()[i];
                string type = table.ColumnTypes()[i];

                type = change(type);

                shared_ptr<Field> field = make_shared<Field>("0",columnName,type);
                fields.push_back(field);

                statementAnalyzer->analysis->setColumn(field, make_shared<TpchColumnHandle>(field->getValue(),
                                                                                            make_shared<Type>(field->getType())));
            }

            statementAnalyzer->analysis->registerTable(node, make_shared<TableHandle>(name->getCatalogName(),name->getSchemaName(),name->getObjectName()));

            return createAndAssignScope("TableScope",node, (Scope*)context, fields);

        }

        shared_ptr<QualifiedObjectName> createQualifiedObjectName(string tableName)
        {
            return QualifiedObjectName::valueOf("tpch_test.tpch_1."+tableName);
        }

        list<Expression*> descriptorToFields(Scope * scope)
        {
            list<Expression*> builder;
            for (int fieldIndex = 0; fieldIndex < scope->getRelationType()->getAllFieldCount(); fieldIndex++) {
                FieldReference *expression = new FieldReference(scope->getRelationType()->getFieldByIndex(fieldIndex)->getNodeLocation(), fieldIndex);
                builder.push_back(expression);
                analyzeExpression(expression, scope);
            }
            return builder;
        }

        void *VisitJoin(Join *node, void *context) override {


            Scope *left = (Scope *) Visit(node->getLeft(), context);
            Scope *right = (Scope *) Visit(node->getRight(), context);


            Scope *output = createAndAssignScope("JoinScope", node, (Scope *) context,
                                                 left->getRelationType()->joinWith(right->getRelationType()));

            if (node->getJoinCriteria() != NULL) {
                checkJoinCriteria(node,output);
            }


            this->statementAnalyzer->analysis->registerJoin(node, NULL);

            return output;
        }

        void checkJoinCriteria(Join *node,Scope *output)
        {
            auto analyzeResult = this->analyzeExpression(
                    dynamic_pointer_cast<JoinOn>(node->getJoinCriteria())->getExpression(), output);
            if (analyzeResult->getCoercions().size() > 0)
                this->statementAnalyzer->exceptionCollector->recordError("Unsupported join key coercion now!");

            if (node->getJoinCriteria()->getJoinCriteriaName() == "JoinOn") {
                Expression *expression = dynamic_pointer_cast<JoinOn>(node->getJoinCriteria())->getExpression();
                if (expression->getExpressionId() == "LogicalBinaryExpression")
                    if (((LogicalBinaryExpression *) expression)->getOp() == "OR")
                        this->statementAnalyzer->exceptionCollector->recordError(
                                "Unsupported 'OR' predicate in Join On now!");

                bool hasNonEqualExpression = false;
                for (auto expression: analyzeResult->getOperators()) {
                    if (expression->getExpressionId() == "ComparisionExpression") {
                        if (((ComparisionExpression *) expression)->getOperator() != ComparisionExpression::EQUAL)
                            hasNonEqualExpression = true;
                    }
                }
                if(hasNonEqualExpression)
                {
                    this->statementAnalyzer->exceptionCollector->recordError("Unsupported complex join predicates now!");
                }
            }
        }


        list<Expression *> analyzeGroupBy(QuerySpecification *node, Scope *scope, list<Expression*> outputExpressions)
        {
            if (node->getGroupBy() != NULL) {
                list<Expression*> groupingExpressions;
                auto groupingElements = node->getGroupBy()->getGroupingElements();

                for(auto element : groupingElements)
                {
                    if(element->getGroupingElementName() == "SimpleGroupBy")
                    {
                        auto groupByExpressions = ((SimpleGroupBy *)element)->getExpressions();
                        for(auto expression : groupByExpressions)
                        {
                            if(((Literal*)expression)->getLiteralId() == "Int32Literal")
                            {
                                long ordinal = ((Int32Literal*)expression)->getValue();
                                if(ordinal < 1 || ordinal > outputExpressions.size()) {
                                    this->statementAnalyzer->exceptionCollector->recordError("Group By position " +
                                                                                             to_string(ordinal) +
                                                                                             " is not in select list");
                                    return  {};
                                }
                            }
                            else if(((Literal*)expression)->getLiteralId() == "Int64Literal")
                            {
                                long ordinal = ((Int64Literal*)expression)->getValue();
                                if(ordinal < 1 || ordinal > outputExpressions.size()) {
                                    this->statementAnalyzer->exceptionCollector->recordError("Group By position " +
                                                                                             to_string(ordinal) +
                                                                                             " is not in select list");
                                    return  {};
                                }
                            }
                            else
                            {
                                analyzeExpression(expression,scope);
                            }

                            groupingExpressions.push_back(expression);
                        }


                    }
                    else
                    {
                        this->statementAnalyzer->exceptionCollector->recordError("Unsupported group by type "+element->getGroupingElementName()+"!");
                        return {};
                    }
                }


                this->statementAnalyzer->analysis->setGroupByExpressions(node,groupingExpressions);
                return groupingExpressions;
            }

            return {};
        }


        Scope * analyzeFrom(QuerySpecification *node, Scope * scope)
        {
            if (node->getFrom() != NULL) {
                return (Scope *)Visit(node->getFrom(), scope);
            }

            auto voidScope = createScope("VoidScope",scope);
            this->statementAnalyzer->analysis->addVoidScope(voidScope);
            return voidScope;
        }

        shared_ptr<ExpressionAnalysis> analyzeExpression(Expression *expression, Scope * scope)
        {
            auto re = statementAnalyzer->expressionAnalyzer->analyzeExpression(scope,expression);

            for(auto et : re->getTypes())
                this->statementAnalyzer->analysis->setType(et.first,et.second);

            for(auto et : re->getCoercions())
                this->statementAnalyzer->analysis->setCoercion(et.first,et.second);

            for(auto et : re->getColumnReferences())
            {
                this->statementAnalyzer->analysis->setColumnReference(et.first,et.second);
            }


            return re;
        }

        void analyzeWhere(Node *node, Scope * scope, Expression *predicate)
        {
            shared_ptr<ExpressionAnalysis>  expressionAnalysis = analyzeExpression(predicate, scope);
            statementAnalyzer->analysis->setWhere(node, predicate);
        }

        list<Expression*> analyzeSelect(QuerySpecification *node, Scope * scope)
        {
            list<Expression*> outputExpressionBuilder;

            list<SelectItem*> selectItems;

            list<FunctionCall *> aggregates;

            if(node->getSelect() != NULL)
            {
                selectItems = node->getSelect()->getSelectItems();
            }
            for (SelectItem *item : selectItems) {
                if (item->getSelectItemId() == "AllColumns") {
                    // expand * and T.*


                    shared_ptr<RelationType> relationType = scope->getRelationType();
                   // List<Field> fields = relationType.resolveFieldsWithPrefix(starPrefix);

                    vector<shared_ptr<Field>> fields = relationType->getAllFields();
                    for (auto field : fields) {
                        int fieldIndex = relationType->indexOf(field);
                        FieldReference *expression = new FieldReference(field->getNodeLocation(), fieldIndex);
                        outputExpressionBuilder.push_back(expression);
                        shared_ptr<ExpressionAnalysis>  expressionAnalysis = analyzeExpression(expression, scope);

                        //Type type = expressionAnalysis.getType(expression);

                    }
                }
                else if (item->getSelectItemId() == "SingleColumn") {
                    SingleColumn *column = (SingleColumn*)item;
                    shared_ptr<ExpressionAnalysis>  expressionAnalysis = analyzeExpression(column->getExpression(), scope);

                    auto functionCalls = expressionAnalysis->getFunctionCalls();
                    auto functionHandles = expressionAnalysis->getFunctionHandles();

                    for(auto function : functionCalls)
                        if(function.second == "AGGREGATE")
                            aggregates.push_back((FunctionCall*)function.first);

                    for(auto functionName : functionHandles)
                    {
                        this->statementAnalyzer->analysis->setAggregateFunctionHandle(functionName.first,functionName.second);
                    }

                    outputExpressionBuilder.push_back(column->getExpression());

                   // Type type = expressionAnalysis.getType(column.getExpression());

                }
                else {
                    this->statementAnalyzer->exceptionCollector->recordError("Unsupported SelectItem type");
                }
            }

            statementAnalyzer->analysis->setAggregates(node,aggregates);
            statementAnalyzer->analysis->setOutputExpressions(node, outputExpressionBuilder);

            return outputExpressionBuilder;
        }





        Scope * computeAndAssignOutputScope(QuerySpecification *node, Scope * scope, Scope * sourceScope)
        {
            vector<shared_ptr<Field>> outputFields;

            list<SelectItem*> selectItems;
            if(node->getSelect() != NULL)
            {
                selectItems = node->getSelect()->getSelectItems();
            }
            for (SelectItem *item : selectItems) {
                if (item->getSelectItemId() == "AllColumns") {
                    // expand * and T.*


                    for (shared_ptr<Field> field : sourceScope->getRelationType()->getAllFields()) {

                        outputFields.push_back(field);
                    }
                }
                else if (item->getSelectItemId() == "SingleColumn") {
                    SingleColumn *column = (SingleColumn*) item;

                    Expression *expression = column->getExpression();
                    //Optional<Identifier> field = column.getAlias();

                   // Optional<QualifiedObjectName> originTable = Optional.empty();
                    //Optional<String> originColumn = Optional.empty();
                    //QualifiedName name = null;

                    string name = "";
                    if (expression->getExpressionId() == "Identifier") {
                     //   name = QualifiedName.of(((Identifier) expression).getValue());
                     name = ((Identifier *)expression)->getValue();
                    }
                    else if(expression->getExpressionId() == "FunctionCall")
                    {
                        name = ((FunctionCall *)expression)->getFuncName();
                    }


                    vector<shared_ptr<Field>> matchingFields = sourceScope->getRelationType()->resolveFields(name);

                   // if (!field.isPresent()) {
                   //     if (name != null) {
                   //         field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
                   //     }
                  //  }

                   // outputFields.push_back(Field.newUnqualified(expression.getLocation(), field.map(Identifier::getValue), analysis.getType(expression), originTable, originColumn, column.getAlias().isPresent())); // TODO don't use analysis as a side-channel. Use outputExpressions to look up the type

                   if(!matchingFields.empty())
                   {
                       for(auto field : matchingFields)
                       {
                           outputFields.push_back(field);
                       }
                   }
                   else
                   {
                       auto ex = this->statementAnalyzer->analysis->getType(expression);

                       if(ex == NULL)
                       {
                           return createAndAssignScope("QuerySpecificationScope",node, scope, outputFields);
                       }
                       else {
                           string type = ex->getType();
                           outputFields.push_back(make_shared<Field>(expression->getLocation(), name, type));
                       }
                   }

                }
                else {
               //     throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
                }
            }

            return createAndAssignScope("QuerySpecificationScope",node, scope, outputFields);
        }

        Scope *createAndAssignScope(string scopeName,Node *node, Scope *parentScope, vector<shared_ptr<Field>> fields)
        {
            return createAndAssignScope(scopeName, node, parentScope, make_shared<RelationType>(fields));
        }

        Scope *createAndAssignScope(string scopeName,Node *node, Scope *parentScope, shared_ptr<RelationType> relationType)
        {
            auto newRelationId = make_shared<RelationId>(node);
            Scope *scope = scopeBuilder(parentScope)
                    ->withRelationType(newRelationId, relationType)
                    ->build(scopeName);

            statementAnalyzer->analysis->setScope(node, scope);
            return scope;
        }


        Scope *createScope(string scopeName,Scope *parentScope) {
            return scopeBuilder(parentScope)->build(scopeName);
        }

        shared_ptr<ScopeBuilder> scopeBuilder(Scope *parentScope) {
            shared_ptr<ScopeBuilder> sBuilder = make_shared<ScopeBuilder>();

            if (parentScope != NULL) {

                sBuilder->withParent(parentScope);
            } else if (outerQueryScope != NULL) {
                sBuilder->withOuterQueryParent(outerQueryScope);
            }

            return sBuilder;
        }



    };

public:
    StatementAnalyzer(Node *root){
        this->root = root;
        this->analysis = make_shared<Analysis>("");
        this->catalogsMetaManager = make_shared<CatalogsMetaManager>();
        this->exceptionCollector = make_shared<ExceptionCollector>();
        this->functionAndTypeResolver = make_shared<FunctionAndTypeResolver>(this->exceptionCollector);
        this->astAllocator = make_shared<AstNodeAllocator>();

        this->expressionAnalyzer = make_shared<ExpressionAnalyzer>(this->analysis,this->functionAndTypeResolver,this->exceptionCollector,this->astAllocator);


    }


    shared_ptr<Analysis> getAnalysis()
    {
        return this->analysis;
    }

    Scope * analyze(Node *node) {

        Scope * outScope = new Scope("BoundaryScope",node,true);
        return (Scope *) make_shared<Visitor>(shared_from_this())->Visit(node, outScope);
    }

    Scope * analyze()
    {
        Scope *outScope = new Scope("BoundaryScope",root,true);
        this->analysis->addVoidScope(outScope);
        shared_ptr<Visitor> stmtAnalyzer = make_shared<Visitor>(outScope,shared_from_this());
        auto outputScope = (Scope*)stmtAnalyzer->Visit(root,outScope);
       // delete(outScope);

       // test();
        return outputScope;
    }

    shared_ptr<ExceptionCollector> getExceptionCollector()
    {
        return this->exceptionCollector;
    }





class A_Rewriter : public  ExpressionTreeRewriter::ExpressionRewriter
{
public:
    Expression *rewriteArithmeticBinary(ArithmeticBinaryExpression *node, void *context,
                                        shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteArithmeticBinary");
        return NULL;
    }

    Expression *rewriteComparisionExpression(ComparisionExpression *node, void *context,
                                             shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteComparisionExpression");
        return NULL;
    }


    Expression *rewriteLogicalBinaryExpression(LogicalBinaryExpression *node, void *context,
                                               shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteLogicalBinaryExpression");
        return NULL;
    }

    Expression *rewriteNotExpression(NotExpression *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteNotExpression");
        return NULL;
    }

    Expression *rewriteFunctionCall(FunctionCall *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {


        vector<Expression*> arguments = treeRewriter->rewrite(node->getChildren(), context);


        return new FunctionCall(node->getLocation(),"avg",arguments);
    }


    Expression *rewriteLiteral(Literal *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {

        return new Identifier(node->getLocation(),"HAHAHA");
    }


    Expression *rewriteIdentifier(Identifier *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteIdentifier");

        return new Int64Literal(node->getLocation(),"12");
    }


    Expression *rewriteCast(Cast *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteCast");
        return NULL;
    }


    Expression *rewriteFieldReference(FieldReference *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteFieldReference");
        return NULL;
    }

    Expression *rewriteSymbolReference(SymbolReference *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
        spdlog::info("rewriteSymbolReference");
        return NULL;
    }
};

    void test()
    {

        shared_ptr<ExpressionTreeRewriter> expressionTreeRewriter = make_shared<ExpressionTreeRewriter>(astAllocator);

        auto expressions = this->analysis->getExpressions();
        shared_ptr<A_Rewriter> aRewriter = make_shared<A_Rewriter>();
        vector<Expression *> changedExpressions;
        for(auto ex : expressions)
        {

            auto re = (Expression*)expressionTreeRewriter->rewriteWith(aRewriter,ex.first,NULL);
            changedExpressions.push_back(re);
        }

        int i = 1;
        i++;

        for(auto ex : changedExpressions)
        {

            AstNodeCleaner astNodeCleaner;
            astNodeCleaner.Visit(ex, NULL);
        }


    }



};






#endif //FRONTEND_STATEMENTANALYZER_HPP
