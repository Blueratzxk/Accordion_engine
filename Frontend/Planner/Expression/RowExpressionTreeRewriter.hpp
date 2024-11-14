//
// Created by zxk on 11/5/24.
//

#ifndef FRONTEND_ROWEXPRESSIONTREEREWRITER_HPP
#define FRONTEND_ROWEXPRESSIONTREEREWRITER_HPP



#include "RowExpressionNodeAllocator.hpp"
#include <vector>
#include "DefaultRowExpressionVisitor.hpp"

class RowExpressionTreeRewriter : public enable_shared_from_this<RowExpressionTreeRewriter>
{


public:
    class RowExpressionRewriter {

    public:
        void *user;
        RowExpressionRewriter()
        {

        }
        RowExpressionRewriter(void *user)
        {
            this->user = user;
        }

        virtual RowExpression *rewriteRowExpression(RowExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) {
            return NULL;
        }


        virtual RowExpression *rewriteCall(CallExpression *node, void *context,
                                                    shared_ptr<RowExpressionTreeRewriter> treeRewriter) {
            return rewriteRowExpression(node, context, treeRewriter);
        }

        virtual  RowExpression *rewriteVariableReferenceExpression(VariableReferenceExpression *node, void *context,
                                                          shared_ptr<RowExpressionTreeRewriter> treeRewriter) {
            return rewriteRowExpression(node, context, treeRewriter);
        }


        virtual  RowExpression *rewriteConstantExpression(ConstantExpression *node, void *context,
                                                            shared_ptr<RowExpressionTreeRewriter> treeRewriter) {
            return rewriteRowExpression(node, context, treeRewriter);
        }

        virtual  RowExpression *rewriteInputReferenceExpression(InputReferenceExpression *node, void *context, shared_ptr<RowExpressionTreeRewriter> treeRewriter) {
            return rewriteRowExpression(node, context, treeRewriter);
        }


    };

private:
    class RewritingVisitor : public DefaultRowExpressionVisitor {

        shared_ptr<RowExpressionRewriter> expressionRewriter;
        shared_ptr<RowExpressionTreeRewriter> expressionTreeRewriter;
        shared_ptr<RowExpressionNodeAllocator> rowExpressionAllocator;
    public:
        RewritingVisitor(shared_ptr<RowExpressionRewriter> rowExpressionRewriter,shared_ptr<RowExpressionTreeRewriter> rowExpressionTreeRewriter) {
            this->expressionRewriter = rowExpressionRewriter;
            this->expressionTreeRewriter = rowExpressionTreeRewriter;
            this->rowExpressionAllocator = rowExpressionTreeRewriter->rowExpressionNodeAllocator;
        }




        void *VisitExpression(RowExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                RowExpression *result = this->expressionRewriter->rewriteRowExpression(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            this->expressionRewriter->rewriteRowExpression(node,context,expressionTreeRewriter);
            return NULL;
        }

        void *VisitConstant(ConstantExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                RowExpression *result = this->expressionRewriter->rewriteConstantExpression(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            return this->rowExpressionAllocator->new_ConstantExpression(node->getSourceLocation(),node->getValue(),node->getType()->getType());
        }

        void * VisitCall(CallExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                RowExpression *result = this->expressionRewriter->rewriteCall(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            vector<RowExpression*> nodeArguments;
            for(auto argument : node->getArguments())
                nodeArguments.push_back(argument.get());


            vector<RowExpression*> arguments = expressionTreeRewriter->rewrite(nodeArguments, context);

            return this->rowExpressionAllocator->new_CallExpression(node->getSourceLocation(),node->getDisplayName(),node->getFunctionHandle(),node->getReturnType(),node->getArguments());

        }

        void *VisitVariableReference(VariableReferenceExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                RowExpression *result = this->expressionRewriter->rewriteVariableReferenceExpression(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            return this->rowExpressionAllocator->new_VariableReferenceExpression(node->getSourceLocation(),node->getName(),node->getType()->getType());
        }

        void * VisitInputReference(InputReferenceExpression *reference, void *context) override
        {
            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                RowExpression *result = this->expressionRewriter->rewriteInputReferenceExpression(reference, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            return this->rowExpressionAllocator->new_InputReferenceExpression(reference->getSourceLocation(),reference->getValue(),reference->getType()->getType());
        }


    };

    shared_ptr<RowExpressionNodeAllocator> rowExpressionNodeAllocator;
    shared_ptr<RewritingVisitor> visitor;
public:
    RowExpressionTreeRewriter(shared_ptr<RowExpressionNodeAllocator> rowExpressionNodeAllocator){
        this->rowExpressionNodeAllocator = rowExpressionNodeAllocator;
    }

    void* rewriteWith(shared_ptr<RowExpressionRewriter> expressionRewriter, RowExpression *node, void *context)
    {

        visitor = make_shared<RewritingVisitor>(expressionRewriter,shared_from_this());
        auto result = this->rewrite(node,context);

        //Cannot delete this code! Or it will cause memory leak because of circle reference of shared_ptr!
        this->visitor = NULL;


        return result;
    }


    void *rewrite(RowExpression* node, void* context)
    {
        auto newContext = new ExpressionTreeRewriterContext(context,false);
        auto result =  visitor->Visit(node, newContext);
        delete newContext;

        return result;
    }
    void *defaultRewrite(RowExpression* node, void* context)
    {
        auto newContext = new ExpressionTreeRewriterContext(context,true);
        auto result =  visitor->Visit(node, newContext);
        delete newContext;

        return result;
    }

    vector<RowExpression*> rewrite(vector<RowExpression*> items, void* context)
    {
        vector<RowExpression *> expressions;
        for (auto expression : items) {

            expressions.push_back((RowExpression*)rewrite((RowExpression*)expression, context));
        }
        return expressions;
    }



    class ExpressionTreeRewriterContext {
        bool defaultRewrite;
        void *context;

    public:
        ExpressionTreeRewriterContext(void *context, bool defaultRewrite) {
            this->context = context;
            this->defaultRewrite = defaultRewrite;
        }

        void *get() {
            return context;
        }

        bool isDefaultRewrite() {
            return defaultRewrite;
        }
    };
};


#endif //FRONTEND_ROWEXPRESSIONTREEREWRITER_HPP
