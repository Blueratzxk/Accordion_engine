//
// Created by zxk on 10/22/24.
//

#ifndef FRONTEND_EXPRESSIONTREEREWRITER_HPP
#define FRONTEND_EXPRESSIONTREEREWRITER_HPP

#include "../../AstNodes/AstNodeAllocator.hpp"
#include "../../AstNodes/DefaultAstExpressionVisitor.hpp"
#include <memory>


class ExpressionTreeRewriter : public enable_shared_from_this<ExpressionTreeRewriter>
{


public:
    class ExpressionRewriter {

    public:
        void *user;
        ExpressionRewriter()
        {

        }
        ExpressionRewriter(void *user)
        {
            this->user = user;
        }

        virtual Expression *rewriteExpression(Expression *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return NULL;
        }


        virtual Expression *rewriteArithmeticBinary(ArithmeticBinaryExpression *node, void *context,
                                            shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }

        virtual  Expression *rewriteComparisionExpression(ComparisionExpression *node, void *context,
                                                 shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }


        virtual  Expression *rewriteLogicalBinaryExpression(LogicalBinaryExpression *node, void *context,
                                                   shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }

        virtual  Expression *rewriteNotExpression(NotExpression *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }

        virtual  Expression *rewriteFunctionCall(FunctionCall *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }


        virtual  Expression *rewriteLiteral(Literal *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }


        virtual  Expression *rewriteIdentifier(Identifier *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }


        virtual  Expression *rewriteCast(Cast *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }


        virtual   Expression *rewriteFieldReference(FieldReference *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }

        virtual  Expression *rewriteSymbolReference(SymbolReference *node, void *context, shared_ptr<ExpressionTreeRewriter> treeRewriter) {
            return rewriteExpression(node, context, treeRewriter);
        }


    };

private:
    class RewritingVisitor : public DefaultAstExpressionVisitor {

        shared_ptr<ExpressionRewriter> expressionRewriter;
        shared_ptr<ExpressionTreeRewriter> expressionTreeRewriter;
        shared_ptr<AstNodeAllocator> astAllocator;
    public:
        RewritingVisitor(shared_ptr<ExpressionRewriter> expressionRewriter,shared_ptr<ExpressionTreeRewriter> expressionTreeRewriter) {
            this->expressionRewriter = expressionRewriter;
            this->expressionTreeRewriter = expressionTreeRewriter;
            this->astAllocator = expressionTreeRewriter->astAllocator;
        }



        void *VisitExpression(Expression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteExpression(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            this->expressionRewriter->rewriteExpression(node,context,expressionTreeRewriter);
            return NULL;
        }

        void *VisitIdentifier(Identifier *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteIdentifier(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            return this->astAllocator->new_Identifier(node->getLocation(),node->getValue());
        }

        void *VisitFunctionCall(FunctionCall *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteFunctionCall(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            vector<Expression*> arguments = expressionTreeRewriter->rewrite(node->getChildren(), context);

            bool isRewrite = false;
            for(int i = 0 ; i < node->getChildren().size() ; i++)
            {
                if(arguments[i] != node->getChildren()[i]) {
                    isRewrite = true;
                  //  delete node->getChildren()[i];
                }
            }


            return this->astAllocator->new_FunctionCall(node->getLocation(),node->getFuncName(),arguments);

        }

        void *VisitLiteral(Literal *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteLiteral(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            return this->astAllocator->record(node->createNewOne());
        }

        void *VisitComparisionExpression(ComparisionExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteComparisionExpression(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }


            Expression *left = (Expression*)this->expressionTreeRewriter->rewrite(node->getLeft(), context);
            Expression *right = (Expression*)this->expressionTreeRewriter->rewrite(node->getRight(), context);

            return this->astAllocator->new_ComparisionExpression(node->getLocation(),node->getOp(), left, right);
        }

        void *VisitLogicalBinaryExpression(LogicalBinaryExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteLogicalBinaryExpression(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            Expression *left = (Expression*)this->expressionTreeRewriter->rewrite(node->getLeft(), context);
            Expression *right = (Expression*)this->expressionTreeRewriter->rewrite(node->getRight(), context);




            return this->astAllocator->new_LogicalBinaryExpression(node->getLocation(),node->getOp(), left, right);

        }

        void *VisitArithmeticBinaryExpression(ArithmeticBinaryExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteArithmeticBinary(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            Expression *left = (Expression*)this->expressionTreeRewriter->rewrite(node->getLeft(), context);
            Expression *right = (Expression*)this->expressionTreeRewriter->rewrite(node->getRight(), context);


            return this->astAllocator->new_ArithmeticBinaryExpression(node->getLocation(),node->getOp(), left, right);
        }

        void *VisitNotExpression(NotExpression *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteNotExpression(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

            Expression *ex = (Expression*)this->expressionTreeRewriter->rewrite(node->getExpression(), context);

            return this->astAllocator->new_NotExpression(node->getLocation(),{ex});
        }

        void *VisitFieldReference(FieldReference *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteFieldReference(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }

           return this->astAllocator->new_FieldReference(node->getLocation(),node->getFieldIndex());
        }

        void *VisitSymbolReference(SymbolReference *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteSymbolReference(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }


            return this->astAllocator->new_SymbolReference(node->getLocation(),node->getName());
        }

        void *VisitCast(Cast *node, void *context) override {

            if (!((ExpressionTreeRewriterContext*)context)->isDefaultRewrite()) {
                Expression *result = this->expressionRewriter->rewriteCast(node, context, expressionTreeRewriter);
                if (result != NULL) {
                    return result;
                }
            }


            Expression *ex = (Expression*)this->expressionTreeRewriter->rewrite(node->getExpression(), context);


            return this->astAllocator->new_Cast(node->getLocation(),{ex},node->getType());

        }

    };

    shared_ptr<AstNodeAllocator> astAllocator;
    shared_ptr<RewritingVisitor> visitor;
public:
    ExpressionTreeRewriter(shared_ptr<AstNodeAllocator> astNodeAllocator){
        this->astAllocator = astNodeAllocator;
    }

    void* rewriteWith(shared_ptr<ExpressionRewriter> expressionRewriter, Node *node, void *context)
    {

        visitor = make_shared<RewritingVisitor>(expressionRewriter,shared_from_this());
        auto result = this->rewrite(node,context);

        //Cannot delete this code! Or it will cause memory leak because of circle reference of shared_ptr!
        this->visitor = NULL;


        return result;
    }


    void *rewrite(Node* node, void* context)
    {
        auto newContext = new ExpressionTreeRewriterContext(context,false);
        auto result =  visitor->Visit(node, newContext);
        delete newContext;

        return result;
    }
    void *defaultRewrite(Node* node, void* context)
    {
        auto newContext = new ExpressionTreeRewriterContext(context,true);
        auto result =  visitor->Visit(node, newContext);
        delete newContext;

        return result;
    }

    vector<Expression*> rewrite(vector<Node*> items, void* context)
    {

        vector<Expression *> expressions;
        for (auto expression : items) {

            expressions.push_back((Expression*)rewrite((Node*)expression, context));
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



#endif //FRONTEND_EXPRESSIONTREEREWRITER_HPP
