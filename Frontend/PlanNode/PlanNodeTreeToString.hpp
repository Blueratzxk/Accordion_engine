//
// Created by zxk on 6/10/23.
//

#ifndef OLVP_PLANNODETREETOSTRING_HPP
#define OLVP_PLANNODETREETOSTRING_HPP



class PlanNodeTreeToString : public NodeVisitor {

private:
public:
    PlanNodeTreeToString() {

    }


    void *VisitExchangeNode(ExchangeNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");


        return node;
    }

    void *VisitFilterNode(FilterNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");



        return node;
    }


    void *VisitFinalAggregationNode(FinalAggregationNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;
    }

    void *VisitLookupJoinNode(LookupJoinNode *node, void *context) override {

        Visit(node->getBuild(),context);
        Visit(node->getProbe(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");


        return node;

    }

    void *VisitCrossJoinNode(CrossJoinNode *node, void *context) override {

        Visit(node->getBuild(),context);
        Visit(node->getProbe(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");


        return node;

    }

    void *VisitSortNode(SortNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");


        return node;
    }

    void *VisitTaskOutputNode(TaskOutputNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;
    }

    void *VisitPartialAggregationNode(PartialAggregationNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");


        return node;
    }


    void *VisitProjectNode(ProjectNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;
    }

    void *VisitLimitNode(LimitNode *node, void *context) override {

        Visit(node->getSource(),context);

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;
    }

    void *VisitRemoteSourceNode(RemoteSourceNode *node, void *context) override {

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;
    }

    void *VisitTableScanNode(TableScanNode *node, void *context) override {

        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;
    }

    void *VisitTopKNode(TopKNode *node, void *context) override {

        Visit(node->getSource(),context);


        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");


        return node;

    }
    void *VisitLocalExchangeNode(LocalExchangeNode *node, void *context) override {

        Visit(node->getSource(),context);


        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;

    }

    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        Visit(node->getSource(),context);


        ((string*)context)->append(node->getType());
        ((string*)context)->append("-");

        return node;
    }

};



#endif //OLVP_PLANNODETREETOSTRING_HPP
