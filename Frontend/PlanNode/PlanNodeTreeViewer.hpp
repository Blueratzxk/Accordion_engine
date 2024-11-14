//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PLANNODETREEVIEWER_HPP
#define OLVP_PLANNODETREEVIEWER_HPP



class PlanNodeTreeViewer : public NodeVisitor {

private:
public:
    PlanNodeTreeViewer() {

    }


    void *VisitExchangeNode(ExchangeNode *node, void *context) override {

        Visit(node->getSource(),NULL);

        cout << node->getType() <<"|"<<node<< endl;

        return node;
    }

    void *VisitFilterNode(FilterNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }


    void *VisitFinalAggregationNode(FinalAggregationNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

    void *VisitLookupJoinNode(LookupJoinNode *node, void *context) override {

        Visit(node->getBuild(),NULL);
        Visit(node->getProbe(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;

    }
    void *VisitCrossJoinNode(CrossJoinNode *node, void *context) override {

        Visit(node->getBuild(),NULL);
        Visit(node->getProbe(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;

    }


    void *VisitSortNode(SortNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

    void *VisitTaskOutputNode(TaskOutputNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

    void *VisitPartialAggregationNode(PartialAggregationNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }


    void *VisitProjectNode(ProjectNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

    void *VisitLimitNode(LimitNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

    void *VisitRemoteSourceNode(RemoteSourceNode *node, void *context) override {

        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

    void *VisitTableScanNode(TableScanNode *node, void *context) override {

        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

    void *VisitTopKNode(TopKNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;

    }
    void *VisitLocalExchangeNode(LocalExchangeNode *node, void *context) override {

        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;

    }

    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        Visit(node->getSource(),NULL);
        cout << node->getType() <<"|"<<node<< endl;
        return node;
    }

};


#endif //OLVP_PLANNODETREEVIEWER_HPP
