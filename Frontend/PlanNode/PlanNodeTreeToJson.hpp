//
// Created by zxk on 6/13/23.
//

#ifndef OLVP_PLANNODETREETOJSON_HPP
#define OLVP_PLANNODETREETOJSON_HPP



class PlanNodeTreeToJson : public NodeVisitor {

private:
public:
    PlanNodeTreeToJson() {

    }


    void *VisitExchangeNode(ExchangeNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }

    void *VisitFilterNode(FilterNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }


    void *VisitFinalAggregationNode(FinalAggregationNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);


        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }

    void *VisitLookupJoinNode(LookupJoinNode *node, void *context) override {

        nlohmann::json *childrenProbe = (nlohmann::json *)Visit(node->getProbe(),context);
        nlohmann::json *childrenBuild = (nlohmann::json *)Visit(node->getBuild(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*childrenProbe);
        (*jsonNode)["children"].push_back(*childrenBuild);


        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(childrenProbe);
        delete(childrenBuild);


        return jsonNode;

    }
    void *VisitCrossJoinNode(CrossJoinNode *node, void *context) override {

        nlohmann::json *childrenProbe = (nlohmann::json *)Visit(node->getProbe(),context);
        nlohmann::json *childrenBuild = (nlohmann::json *)Visit(node->getBuild(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*childrenProbe);
        (*jsonNode)["children"].push_back(*childrenBuild);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(childrenProbe);
        delete(childrenBuild);


        return jsonNode;

    }

    void *VisitSortNode(SortNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }

    void *VisitTaskOutputNode(TaskOutputNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);


        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }

    void *VisitPartialAggregationNode(PartialAggregationNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }


    void *VisitProjectNode(ProjectNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }

    void *VisitLimitNode(LimitNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);


        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }


    void *VisitRemoteSourceNode(RemoteSourceNode *node, void *context) override {

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        return jsonNode;
    }

    void *VisitTableScanNode(TableScanNode *node, void *context) override {



        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType() +"("+node->getDescriptor().getTable()+")";

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType() +"("+node->getDescriptor().getTable()+")"+"\n"+node->getLabel();

        return jsonNode;
    }

    void *VisitTopKNode(TopKNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;

    }
    void *VisitLocalExchangeNode(LocalExchangeNode *node, void *context) override {

        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;

    }

    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        nlohmann::json *children = (nlohmann::json *)Visit(node->getSource(),context);

        nlohmann::json *jsonNode = new nlohmann::json ();
        (*jsonNode)["name"] = node->getType();
        (*jsonNode)["children"].push_back(*children);

        if(node->getLabel() != "")
            (*jsonNode)["name"] = node->getType()+"\n"+node->getLabel();

        delete(children);


        return jsonNode;
    }


    string getJson(PlanNode *root)
    {
        nlohmann::json *children = (nlohmann::json *)this->Visit(root,NULL);
        string result = children->dump();
        delete(children);
        return result;

    }
    nlohmann::json getJsonObj(PlanNode *root)
    {
        nlohmann::json *children = (nlohmann::json *)this->Visit(root,NULL);
        nlohmann::json json = *children;
        delete(children);

        return json;
    }

};




#endif //OLVP_PLANNODETREETOJSON_HPP
