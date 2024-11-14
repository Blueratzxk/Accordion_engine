//
// Created by zxk on 11/7/24.
//

#ifndef FRONTEND_PUSHDOWNPREDICATES_HPP
#define FRONTEND_PUSHDOWNPREDICATES_HPP

#include "PlanOptimizer.h"
#include "SimplePlanNodeRewriter.hpp"
#include "../Planner/Expression/RowExpressionTreeRewriter.hpp"

class PushDownPredicates : public PlanOptimizer
{

    class RowRewriter;

    class RewriteContext
    {
        shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter;
        map<string,set<string>> rewriteNames;
        shared_ptr<RowRewriter> rowRewriter;
    public:
        RewriteContext(shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter)
        {
            this->rowExpressionTreeRewriter = rowExpressionTreeRewriter;
            this->rowRewriter = make_shared<RowRewriter>();
        }

        string getRewriteName(string origin)
        {
            for(auto names : this->rewriteNames)
            {
                if(names.second.contains(origin))
                    return names.first;
            }
            return origin;
        }

        RowExpression* rewrite(RowExpression *node, void *context)
        {
            return (RowExpression*)this->rowExpressionTreeRewriter->rewriteWith(rowRewriter,node,context);
        }


        void addRewriteMap(string origin,string newName)
        {

            string originName = origin;
            auto rewriteName = getRewriteName(originName);
            if(rewriteName != originName)
                originName = rewriteName;

            if(rewriteNames.contains(originName))
                rewriteNames[originName].insert(newName);
            else
                rewriteNames[originName] = {newName};
        }

    };

    class RowRewriter : public  RowExpressionTreeRewriter::RowExpressionRewriter
    {
    public:

        RowExpression * rewriteVariableReferenceExpression(VariableReferenceExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) override
        {
            auto rewriteContext = (RewriteContext*)((RowExpressionTreeRewriter::ExpressionTreeRewriterContext*)context)->get();
            return new VariableReferenceExpression(node->getSourceLocation(),rewriteContext->getRewriteName(node->getName()),node->getType()->getType());
        }

        RowExpression * rewriteCall(CallExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) override
        {

            auto rewriteContext = (RewriteContext*)((RowExpressionTreeRewriter::ExpressionTreeRewriterContext*)context)->get();

            auto arguments = node->getArguments();

            list<shared_ptr<RowExpression>> rewrittenResults;
            for(auto argument : arguments)
            {
                auto re = treeRewriter->rewrite(argument.get(),rewriteContext);
                rewrittenResults.push_back(shared_ptr<RowExpression>((RowExpression*)re));
            }

            return new CallExpression(node->getSourceLocation(),node->getDisplayName(),node->getFunctionHandle(),node->getReturnType(),rewrittenResults);
        }

        RowExpression * rewriteInputReferenceExpression(InputReferenceExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) override
        {
            return node;
        }
        RowExpression * rewriteConstantExpression(ConstantExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) override
        {
            return node;
        }


    };



    shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter;
public:
    PushDownPredicates()
    {

    }

    PlanNode* optimize(PlanNode *plan,shared_ptr<PlanNodeAllocator> planNodeAllocator,
                       shared_ptr<VariableAllocator> variableAllocator, shared_ptr<PlanNodeIdAllocator> idAllocator)
    {

        this->rowExpressionTreeRewriter = make_shared<RowExpressionTreeRewriter>(make_shared<RowExpressionNodeAllocator>());
        RewriteContext *context = new RewriteContext(this->rowExpressionTreeRewriter);



        shared_ptr<Visitor> visitor = make_shared<Visitor>(variableAllocator,planNodeAllocator);
        auto result = (PlanNode *)visitor->Visit(plan,context);


        delete context;
        return result;
    }


    class Visitor : public SimplePlanNodeRewriter {

        shared_ptr<VariableAllocator> variableAllocator;
        shared_ptr<PlanNodeAllocator> planNodeAllocator;
    public:
        Visitor(shared_ptr<VariableAllocator> variableAllocator,shared_ptr<PlanNodeAllocator> planNodeAllocator) {
            this->variableAllocator = variableAllocator;
            this->planNodeAllocator = planNodeAllocator;
        }


        void *VisitExchangeNode(ExchangeNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});
        }

        void *VisitFilterNode(FilterNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            auto rewriteContext = ((RewriteContext*)context);
            auto rowExpressionRewritten = rewriteContext->rewrite(node->getPredicates().get(),rewriteContext);

            auto newNode = this->planNodeAllocator->new_FilterNode(node->getSourceLocation(),node->getId(),node->getSource(),shared_ptr<RowExpression>(rowExpressionRewritten));

            return newNode->replaceChildren({rewrittenNode});
        }

        void *VisitFinalAggregationNode(FinalAggregationNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);
            return node->replaceChildren({rewrittenNode});;
        }

        void *VisitLookupJoinNode(LookupJoinNode *node, void *context) override {

            PlanNode *rewrittenNodeBuild = (PlanNode *) Visit(node->getBuild(), context);
            PlanNode *rewrittenNodeProbe = (PlanNode *) Visit(node->getProbe(), context);

            auto rewriteContext = ((RewriteContext*)context);

            auto joinClause = node->getJoinClauses();
            list<shared_ptr<EquiJoinClause>> newJoinClause;

            for(auto joinClause : joinClause) {
                auto rowExpressionRewrittenLeft = rewriteContext->rewrite(joinClause->getLeft().get(), context);
                auto rowExpressionRewrittenRight = rewriteContext->rewrite(joinClause->getRight().get(), context);

                auto left = shared_ptr<VariableReferenceExpression>(((VariableReferenceExpression*)rowExpressionRewrittenLeft));
                auto right = shared_ptr<VariableReferenceExpression>(((VariableReferenceExpression*)rowExpressionRewrittenRight));
                newJoinClause.push_back(make_shared<EquiJoinClause>(left,right));
            }

            auto newJoin = this->planNodeAllocator->new_LookupJoinNode(node->getId(),rewrittenNodeProbe,rewrittenNodeBuild,newJoinClause,node->getOutputVariables());

            return newJoin->replaceChildren({rewrittenNodeProbe, rewrittenNodeBuild});

        }

        void *VisitCrossJoinNode(CrossJoinNode *node, void *context) override {

            PlanNode *rewrittenNodeBuild = (PlanNode *) Visit(node->getBuild(), context);
            PlanNode *rewrittenNodeProbe = (PlanNode *) Visit(node->getProbe(), context);

            return node->replaceChildren({rewrittenNodeProbe, rewrittenNodeBuild});;

        }

        void *VisitSortNode(SortNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});;
        }

        void *VisitTaskOutputNode(TaskOutputNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});;
        }

        void *VisitPartialAggregationNode(PartialAggregationNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});;
        }


        void *VisitProjectNode(ProjectNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);


            auto rewriteContext = ((RewriteContext*)context);

            auto results = extractRedundantColumn(node->getAssignments());

            for(auto name : results)
                rewriteContext->addRewriteMap(name.second,name.first);


            shared_ptr<AssignmentsBuilder> assignmentsBuilder = make_shared<AssignmentsBuilder>();

            auto assignments = node->getAssignments();
            for(auto assignment : assignments->getMap())
            {
                auto rowExpressionRewrittenRight = rewriteContext->rewrite(assignment.second.get(),context);
                auto rowExpressionRewrittenLeft = rewriteContext->rewrite(assignment.first.get(),context);

                assignmentsBuilder->put(shared_ptr<VariableReferenceExpression>((VariableReferenceExpression*)rowExpressionRewrittenLeft),shared_ptr<RowExpression>(rowExpressionRewrittenRight));
            }

            auto newNode = planNodeAllocator->new_ProjectNode(node->getId(),node->getSource(),assignmentsBuilder->build());
            newNode->addSource(rewrittenNode);
            return newNode;
        }

        void *VisitLimitNode(LimitNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});;
        }

        void *VisitRemoteSourceNode(RemoteSourceNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});;

        }

        void *VisitTableScanNode(TableScanNode *node, void *context) override {



            return node;
        }

        void *VisitTopKNode(TopKNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});;
        }

        void *VisitLocalExchangeNode(LocalExchangeNode *node, void *context) override {

            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);

            return node->replaceChildren({rewrittenNode});;

        }

        void *VisitAggregationNode(AggregationNode *node, void *context) override {
            PlanNode *rewrittenNode = (PlanNode *) Visit(node->getSource(), context);


            auto rewriteContext = ((RewriteContext*)context);
            auto aggs = node->getAggregations();

            map<shared_ptr<VariableReferenceExpression>,shared_ptr<Aggregation>> newAggregations;

            for(auto agg : aggs)
            {
                auto newCall = rewriteContext->rewrite(agg.second->getCall().get(),context);
                shared_ptr<Aggregation> newAggregation = make_shared<Aggregation>(shared_ptr<CallExpression>((CallExpression*)newCall));
                newAggregations[agg.first] = (newAggregation);
            }

            list<shared_ptr<VariableReferenceExpression>> newGroupByKeys;

            for(auto groupByKey : node->getGroupByKeys())
            {
                auto re = rewriteContext->rewrite(groupByKey.get(),context);
                newGroupByKeys.push_back(shared_ptr<VariableReferenceExpression>((VariableReferenceExpression*)re));
            }

            auto  newNode = this->planNodeAllocator->new_AggregationNode(node->getId(),node->getSource(),newAggregations,newGroupByKeys);

            return newNode->replaceChildren({rewrittenNode});
        }


        map<string,string> extractRedundantColumn(shared_ptr<Assignments> assignments)
        {
            map<string,string> redundantColumnNames;
            map<shared_ptr<VariableReferenceExpression>, shared_ptr<RowExpression>> map = assignments->getMap();

            for(auto mapping : map)
            {
                auto originName = mapping.second;
                auto newName = mapping.first;

                if(originName->getExpressionName() == "VariableReferenceExpression")
                {
                    string nname;

                    bool hasSpliter = variableAllocator->hasSpliter(newName->getNameOrValue());

                    if(hasSpliter)
                        nname = variableAllocator->extractPrefix(newName->getNameOrValue(),variableAllocator->getSpliter(newName->getNameOrValue()));
                    else
                        nname = newName->getNameOrValue();

                    string oname;
                    bool hasSpliter2 = variableAllocator->hasSpliter(originName->getNameOrValue());
                    if(hasSpliter2)
                        oname = variableAllocator->extractPrefix(originName->getNameOrValue(),variableAllocator->getSpliter(originName->getNameOrValue()));
                    else
                        oname = originName->getNameOrValue();



                    if(oname == nname)
                        redundantColumnNames[newName->getNameOrValue()] = originName->getNameOrValue();
                }
            }
            return redundantColumnNames;

        }


    };


};



#endif //FRONTEND_PUSHDOWNPREDICATES_HPP
