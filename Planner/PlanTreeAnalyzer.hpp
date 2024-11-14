//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_PLANTREEANALYZER_HPP
#define OLVP_PLANTREEANALYZER_HPP


#include "Fragmenter.hpp"

class PlanTreeAnalyzer
{
    PlanNode *root;
    Fragmenter fragmenter;
    PlanNodeTreeViewer viewer;


public:
    PlanTreeAnalyzer(PlanNode *root){
        this->root = root;
        fragmenter.setRoot(root);
    }

    PlanTreeAnalyzer(){
        root = getAtestTree();
        fragmenter.setRoot(root);
    }

    void analyze()
    {

        viewer.Visit(root,NULL);
        cout <<"---+++++++++++++++++++++++++-"<<endl;
        fragmenter.doFragment();
        vector<PlanFragment> frags = fragmenter.getFragments();
        for(int i = 0 ; i < frags.size() ; i++)
        {
            viewer.Visit(frags[i].getRoot(),NULL);
            cout << "-------------------------------"<<endl;
        }


    }

    std::shared_ptr<SubPlan> analyzeToSubPlanTree()
    {

       // viewer.Visit(root,NULL);
        //cout <<"---+++++++++++++++++++++++++-"<<endl;
        fragmenter.doFragment();
        vector<PlanFragment> frags = fragmenter.getFragments();

        for(int i = 0 ; i < frags.size() ; i++)
        {
    //        viewer.Visit(frags[i].getRoot(),NULL);
     //       cout << "-------------------------------"<<endl;
        }

        return fragmenter.getSubPlan();


    }


    PlanNode* getAtestTree()
    {


        TableScanNode *tableScanProbe = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));
        TableScanNode *tableScanBuild = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));




        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::FIXED_BROADCAST_DISTRIBUTION,{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange",ExchangeNode::REPLICATE,schemeBuild,tableScanBuild);


        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::SCALED_SIMPLE_DISTRIBUTION_BUF,{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange",ExchangeNode::REPARTITION,schemeProbe,tableScanProbe);


        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(probeExchange);




        vector<FieldDesc> inputProbeSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_address","string"),FieldDesc("s_nationkey","int64"),FieldDesc("s_phone","string"),FieldDesc("s_acctbal","double"),FieldDesc("s_comment","string")};
        vector<FieldDesc> inputBuildSchema = {FieldDesc("s_suppkey2","int64"),FieldDesc("s_name2","string"),FieldDesc("s_address2","string"),FieldDesc("s_nationkey2","int64"),FieldDesc("s_phone2","string"),FieldDesc("s_acctbal2","double"),FieldDesc("s_comment2","string")};
        vector<FieldDesc> outputSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_nationkey","int64")};
        vector<int> tprobeOutputChannels = {0,3};
        vector<int> tprobeHashChannels = {3};
        vector<int> tbuildOutputChannels = {0,3};
        vector<int> tbuildHashChannels = {3};
        LookupJoinDescriptor lookupJoinDescriptor(inputProbeSchema,tprobeHashChannels,tprobeOutputChannels,inputBuildSchema,tbuildOutputChannels,tbuildHashChannels,outputSchema);
        LookupJoinNode *lookupJoin = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);
        lookupJoin->addBuild(buildExchange);
        lookupJoin->addProbe(probeLocalExchange);


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"s_suppkey",/*outputName=*/"s_nationkey_all"),
                              AggregateDesc(/*functionName=*/"count",/*inputKey=*/"s_suppkey",/*outputName=*/"s_nationkey_count")
                             },
                /*groupByKeys=*/{});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);
        partialAggregationNode->addSource(lookupJoin);



        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(partialAggregationNode);


        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::SINGLE_DISTRIBUTION,{}));
        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,paggLocalExchange);






        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"s_nationkey_all",/*outputName=*/"s_nationkey_alll"),
                                   AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"s_nationkey_count",/*outputName=*/"s_nationkey_countt")
                                  },
                /*groupByKeys=*/{});

        FinalAggregationNode *fagg = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);
        fagg->addSource(exchange);




        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(fagg);
        return (PlanNode*)output;

    }



};







#endif //OLVP_PLANTREEANALYZER_HPP
