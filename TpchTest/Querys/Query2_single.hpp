//
// Created by zxk on 5/28/24.
//

#ifndef OLVP_QUERY2_SINGLE_HPP
#define OLVP_QUERY2_SINGLE_HPP


#include "../../Query/RegQuery.h"

/*
 *
select
    supplier.acctbal, supplier.name, nation.name, part.partkey, part.mfgr, supplier.address, supplier.phone, supplier.comment
from
    part, supplier, partsupp, nation, region
where
    part.partkey = partsupp.partkey
    and supplier.suppkey = partsupp.suppkey
    and part.size = 30
    and part.type like '%MEDIUM'
    and supplier.nationkey = nation.nationkey
    and nation.regionkey = region.regionkey
    and region.name = 'AMERICA'
    and partsupp.supplycost = (
        select
            min(partsupp.supplycost)
        from
            partsupp, supplier, nation, region
        where
            part.partkey = partsupp.partkey
            and supplier.suppkey = partsupp.suppkey
            and supplier.nationkey = nation.nationkey
            and nation.regionkey = region.regionkey
            and region.name = 'AMERICA'
    )
order by
    supplier.acctbal desc,
    nation.name,
    supplier.name,
    part.partkey;

 */




class Query2_single:public RegQuery
{

public:
    Query2_single(){

    }
    string getSql()  {return TpchSqls::Q2();}
    PlanNode* getPlanTree()
    {

        LookupJoinNode *allJoin = createFinalAgg_FiveJoin();

        LocalExchangeNode *joinLocalExchange = new LocalExchangeNode("joinLocalExchange");
        joinLocalExchange->addSource(allJoin);

        shared_ptr<PartitioningScheme> scheme2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));

        ExchangeNode *exchange = new ExchangeNode("exchange",ExchangeNode::GATHER,scheme2,allJoin);

        FilterNode *filterCost = createAllJoinFilter();
        filterCost->addSource(exchange);

        vector<string> sortKeys = {"s_acctbal","n_name","s_name","ps_partkey"};
        vector<TopKOperator::SortOrder> sortOrders = {TopKOperator::Descending,TopKOperator::Ascending,TopKOperator::Ascending,TopKOperator::Ascending};

        TopKDescriptor topkDescriptor(100,sortKeys,sortOrders);
        TopKNode *topk = new TopKNode(UUID::create_uuid(),topkDescriptor);
        topk->addSource(filterCost);








        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(topk);
        return (PlanNode*)output;
    }

    LookupJoinNode *createRegionJoinNationJoinSupplierJoinPartsupp()
    {

        // region build,nation probe

        TableScanNode *tableScanRegion = createTableScanRegionToBuild();


        Column *r_name = new Column("0","r_name","string");
        StringLiteral *rNameLiteral = new StringLiteral("0","AMERICA");
        FunctionCall *rnameEqual = new FunctionCall("0","equal","bool");
        rnameEqual->addChilds({r_name,rNameLiteral});

        vector<FieldDesc> fieldsIn = {FieldDesc("r_regionkey","int64"),
                                      FieldDesc("r_name","string"),
                                      FieldDesc("r_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,rnameEqual);
        FilterNode *filterRegion = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterRegion->addSource(tableScanRegion);


        ExchangeNode *tableScanNation = createTableScanNationToProbe();

        shared_ptr<PartitioningScheme> schemeBuild1 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange1 = new ExchangeNode("buildExchange111",ExchangeNode::REPLICATE,schemeBuild1,filterRegion);


        //only provide build output schema, probe use its origin input schema as output schema.

        vector<FieldDesc> inputProbeSchema = {FieldDesc("n_nationkey","int64"),FieldDesc("n_name","string"),FieldDesc("n_regionkey","int64"),FieldDesc("n_comment","string")};
        vector<FieldDesc> inputBuildSchema = {FieldDesc("r_regionkey","int64"),FieldDesc("r_name","string"),FieldDesc("r_comment","string")};
        vector<FieldDesc> buildOutputSchema = {FieldDesc("r_regionkey","int64")};
        vector<int> tprobeOutputChannels = {0};
        vector<int> tprobeHashChannels = {2};
        vector<int> tbuildOutputChannels = {0};
        vector<int> tbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(inputProbeSchema,tprobeHashChannels,tprobeOutputChannels,inputBuildSchema,tbuildHashChannels,tbuildOutputChannels,buildOutputSchema);
        LookupJoinNode *regionNationJoin = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);


        LocalExchangeNode *nationLocalExchange = new LocalExchangeNode("nationLocalExchange");
        nationLocalExchange->addSource(tableScanNation);

        regionNationJoin->addBuild(buildExchange1);
        regionNationJoin->addProbe(tableScanNation);

        LocalExchangeNode *regionNationJoinLocalExchange = new LocalExchangeNode("regionNationJoinLocalExchange");
        regionNationJoinLocalExchange->addSource(regionNationJoin);

        shared_ptr<PartitioningScheme> schemeBuild2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange2 = new ExchangeNode("buildExchange222",ExchangeNode::REPLICATE,schemeBuild2,regionNationJoin);

        //-------------------------------join supplier------------------------------------------------------//


        ExchangeNode *tableScanSupplier = createTableScanSupplierToProbe();


        vector<FieldDesc> supplierProbeSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_address","int64"),FieldDesc("s_nationkey","int64"),FieldDesc("s_phone","string"),FieldDesc("s_acctbal","double"),FieldDesc("s_comment","string")};
        vector<FieldDesc> regionNationBuildSchema = {FieldDesc("n_nationkey","int64"),FieldDesc("r_regionkey","int64")};
        vector<FieldDesc> regionNationBuildOutputSchema = {FieldDesc("n_nationkey","int64"),FieldDesc("r_regionkey","int64")};
        vector<int> sprobeOutputChannels = {0};
        vector<int> sprobeHashChannels = {3};
        vector<int> rnbuildOutputChannels = {0,1};
        vector<int> rnbuildHashChannels = {0};
        LookupJoinDescriptor RNSLookupJoinDescriptor(supplierProbeSchema,sprobeHashChannels,sprobeOutputChannels,regionNationBuildSchema,rnbuildHashChannels,rnbuildOutputChannels,regionNationBuildOutputSchema);
        LookupJoinNode *RNSupplierJoin = new LookupJoinNode(UUID::create_uuid(),RNSLookupJoinDescriptor);


        LocalExchangeNode *supplierLocalExchange = new LocalExchangeNode("supplierLocalExchange");
        supplierLocalExchange->addSource(tableScanSupplier);

        RNSupplierJoin->addProbe(tableScanSupplier);
        RNSupplierJoin->addBuild(buildExchange2);

        LocalExchangeNode *RNSupplierJoinLocalExchange = new LocalExchangeNode("RNSupplierJoinLocalExchange");
        RNSupplierJoinLocalExchange->addSource(RNSupplierJoin);

        shared_ptr<PartitioningScheme> schemeBuild3 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange3 = new ExchangeNode("buildExchange333",ExchangeNode::REPLICATE,schemeBuild3,RNSupplierJoin);
        //----------------------------------join partsupp------------------------------------------------------------------------//


        PlanNode *tableScanPartsupp = createTableScanPartSuppToProbe();



        vector<FieldDesc> partsuppProbeSchema = {FieldDesc("ps_partkey","int64"),FieldDesc("ps_suppkey","int64"),FieldDesc("ps_availqty","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("ps_comment","string")};
        vector<FieldDesc> regionNationSupplierBuildSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("n_nationkey","int64"),FieldDesc("r_regionkey","int64")};
        vector<FieldDesc> regionNationSupplierBuildOutputSchema = {FieldDesc("s_suppkey","int64")};
        vector<int> psprobeOutputChannels = {0,3};
        vector<int> psprobeHashChannels = {1};
        vector<int> rnsbuildOutputChannels = {0};
        vector<int> rnsbuildHashChannels = {0};
        LookupJoinDescriptor RNSPLookupJoinDescriptor(partsuppProbeSchema,psprobeHashChannels,psprobeOutputChannels,regionNationSupplierBuildSchema,rnsbuildHashChannels,rnsbuildOutputChannels,regionNationSupplierBuildOutputSchema);
        LookupJoinNode *RNSPartsuppJoin = new LookupJoinNode(UUID::create_uuid(),RNSPLookupJoinDescriptor);


        LocalExchangeNode *PartsuppLocalExchange = new LocalExchangeNode("PartsuppLocalExchange");
        PartsuppLocalExchange->addSource(tableScanPartsupp);

        RNSPartsuppJoin->addProbe(tableScanPartsupp);
        RNSPartsuppJoin->addBuild(buildExchange3);





        return RNSPartsuppJoin;

    }



    LookupJoinNode *create3TJ_RegionJoinNationJoinSupplier()
    {

        // region build,nation probe

        TableScanNode *tableScanRegion = createTableScanRegionToBuild();


        Column *r_name = new Column("0","r_name","string");
        StringLiteral *rNameLiteral = new StringLiteral("0","AMERICA");
        FunctionCall *rnameEqual = new FunctionCall("0","equal","bool");
        rnameEqual->addChilds({r_name,rNameLiteral});

        vector<FieldDesc> fieldsIn = {FieldDesc("r_regionkey","int64"),
                                      FieldDesc("r_name","string"),
                                      FieldDesc("r_comment","string")};

        FilterDescriptor filterDescriptor(fieldsIn,rnameEqual);
        FilterNode *filterRegion = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterRegion->addSource(tableScanRegion);

        shared_ptr<PartitioningScheme> schemeBuild1 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange1 = new ExchangeNode("buildExchange444",ExchangeNode::REPLICATE,schemeBuild1,filterRegion);

        ExchangeNode *tableScanNation = createTableScanNationToProbe();


        //only provide build output schema, probe use its origin input schema as output schema.

        vector<FieldDesc> inputProbeSchema = {FieldDesc("n_nationkey","int64"),FieldDesc("n_name","string"),FieldDesc("n_regionkey","int64"),FieldDesc("n_comment","string")};
        vector<FieldDesc> inputBuildSchema = {FieldDesc("r_regionkey","int64"),FieldDesc("r_name","string"),FieldDesc("r_comment","string")};
        vector<FieldDesc> buildOutputSchema = {FieldDesc("r_regionkey","int64")};
        vector<int> tprobeOutputChannels = {0,1};
        vector<int> tprobeHashChannels = {2};
        vector<int> tbuildOutputChannels = {0};
        vector<int> tbuildHashChannels = {0};
        LookupJoinDescriptor lookupJoinDescriptor(inputProbeSchema,tprobeHashChannels,tprobeOutputChannels,inputBuildSchema,tbuildHashChannels,tbuildOutputChannels,buildOutputSchema);
        LookupJoinNode *regionNationJoin = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);

        LocalExchangeNode *nationProbeLocalExchange = new LocalExchangeNode("nationProbeLocalExchange");
        nationProbeLocalExchange->addSource(tableScanNation);

        regionNationJoin->addBuild(buildExchange1);
        regionNationJoin->addProbe(tableScanNation);

        LocalExchangeNode *NJoinRLocalExchange = new LocalExchangeNode("NJoinRLocalExchange");
        NJoinRLocalExchange->addSource(regionNationJoin);

        shared_ptr<PartitioningScheme> schemeBuild2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange2 = new ExchangeNode("buildExchange555",ExchangeNode::REPLICATE,schemeBuild2,regionNationJoin);

        //-------------------------------join supplier------------------------------------------------------//


        ExchangeNode *tableScanSupplier = createTableScanSupplierToProbe();


        vector<FieldDesc> supplierProbeSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_address","int64"),FieldDesc("s_nationkey","int64"),FieldDesc("s_phone","string"),FieldDesc("s_acctbal","double"),FieldDesc("s_comment","string")};
        vector<FieldDesc> regionNationBuildSchema = {FieldDesc("n_nationkey","int64"),FieldDesc("n_name","string"),FieldDesc("r_regionkey","int64")};
        vector<FieldDesc> regionNationBuildOutputSchema = {FieldDesc("n_nationkey","int64"),FieldDesc("n_name","string"),FieldDesc("r_regionkey","int64")};
        vector<int> sprobeOutputChannels = {0,1,2,4,5,6};
        vector<int> sprobeHashChannels = {3};
        vector<int> rnbuildOutputChannels = {0,1,2};
        vector<int> rnbuildHashChannels = {0};
        LookupJoinDescriptor RNSLookupJoinDescriptor(supplierProbeSchema,sprobeHashChannels,sprobeOutputChannels,regionNationBuildSchema,rnbuildHashChannels,rnbuildOutputChannels,regionNationBuildOutputSchema);
        LookupJoinNode *RNSupplierJoin = new LookupJoinNode(UUID::create_uuid(),RNSLookupJoinDescriptor);

        LocalExchangeNode *supplierProbeLocalExchange = new LocalExchangeNode("supplierProbeLocalExchange");
        supplierProbeLocalExchange->addSource(tableScanSupplier);

        RNSupplierJoin->addProbe(tableScanSupplier);
        RNSupplierJoin->addBuild(buildExchange2);


        return RNSupplierJoin;

    }




    LookupJoinNode *create2TJ_PartJoinPartsupp()
    {

        // part build,partsupp probe

        TableScanNode *tableScanPart = this->createTableScanPartToBuild();


        Column *p_type = new Column("0","p_type","string");
        StringLiteral *pTypeLiteral = new StringLiteral("0","%MEDIUM%");
        FunctionCall *pTypelike = new FunctionCall("0","like","bool");
        pTypelike->addChilds({p_type,pTypeLiteral});

        Column *p_size = new Column("0","p_size","int64");
        Int64Literal *pSizeLiteral = new Int64Literal("0","30");
        FunctionCall *pSizeEqual = new FunctionCall("0","equal","bool");
        pSizeEqual->addChilds({p_size,pSizeLiteral});

        FunctionCall *psizeAndpType = new FunctionCall("0","and","bool");
        psizeAndpType->addChilds({pTypelike,pSizeEqual});



        vector<FieldDesc> fieldsIn = {FieldDesc("p_partkey","int64"),
                                      FieldDesc("p_name","string"),
                                      FieldDesc("p_mfgr","string"),
                                      FieldDesc("p_brand","string"),
                                      FieldDesc("p_type","string"),
                                      FieldDesc("p_size","int64"),
                                      FieldDesc("p_container","string"),
                                      FieldDesc("p_retailprice","double"),
                                      FieldDesc("p_comment","string")};




        FilterDescriptor filterDescriptor(fieldsIn,psizeAndpType);
        FilterNode *filterPart = new FilterNode(UUID::create_uuid(),filterDescriptor);
        filterPart->addSource(tableScanPart);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange666",ExchangeNode::REPLICATE,schemeBuild,filterPart);
        //----------------------------------join partsupp------------------------------------------------------------------------//


        PlanNode *tableScanPartsupp = this->createTableScanPartSuppToProbe();



        vector<FieldDesc> partsuppProbeSchema = {FieldDesc("ps_partkey","int64"),FieldDesc("ps_suppkey","int64"),FieldDesc("ps_availqty","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("ps_comment","string")};

        vector<FieldDesc> partBuildSchema = {FieldDesc("p_partkey","int64"),
                                             FieldDesc("p_name","string"),
                                             FieldDesc("p_mfgr","string"),
                                             FieldDesc("p_brand","string"),
                                             FieldDesc("p_type","string"),
                                             FieldDesc("p_size","int64"),
                                             FieldDesc("p_container","string"),
                                             FieldDesc("p_retailprice","double"),
                                             FieldDesc("p_comment","string")};
        vector<FieldDesc> partBuildOutputSchema = {FieldDesc("p_mfgr","string")};
        vector<int> psprobeOutputChannels = {0,1,3};
        vector<int> psprobeHashChannels = {0};
        vector<int> partbuildOutputChannels = {2};
        vector<int> partbuildHashChannels = {0};
        LookupJoinDescriptor ppsLookupJoinDescriptor(partsuppProbeSchema,psprobeHashChannels,psprobeOutputChannels,partBuildSchema,partbuildHashChannels,partbuildOutputChannels,partBuildOutputSchema);
        LookupJoinNode *partPartsuppJoin = new LookupJoinNode(UUID::create_uuid(),ppsLookupJoinDescriptor);

        LocalExchangeNode *partsuppProbeLocalExchange = new LocalExchangeNode("partsuppProbeLocalExchange");
        partsuppProbeLocalExchange->addSource(tableScanPartsupp);

        partPartsuppJoin->addProbe(tableScanPartsupp);
        partPartsuppJoin->addBuild(buildExchange);


        return partPartsuppJoin;

    }

    LookupJoinNode *create5TJ()
    {

        LookupJoinNode *twoTJ = this->create2TJ_PartJoinPartsupp();
        LookupJoinNode *threeTJ = this->create3TJ_RegionJoinNationJoinSupplier();

        LocalExchangeNode *threeBuildLocalExchange = new LocalExchangeNode("threeBuildLocalExchange");
        threeBuildLocalExchange->addSource(threeTJ);

        LocalExchangeNode *twoJoinLocalExchange = new LocalExchangeNode("twoJoinLocalExchange");
        twoJoinLocalExchange->addSource(twoTJ);

        shared_ptr<PartitioningScheme> schemeBuild = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange = new ExchangeNode("buildExchange777",ExchangeNode::REPLICATE,schemeBuild,threeTJ);


        vector<FieldDesc> twoTJProbeSchema = {FieldDesc("ps_partkey","int64"),FieldDesc("ps_suppkey","int64"),FieldDesc("ps_supplycost","double"),FieldDesc("p_mfgr","string")};




        vector<FieldDesc> threeTJBuildSchema = {
                FieldDesc("s_suppkey","int64"),
                FieldDesc("s_name","string"),
                FieldDesc("s_address","string"),
                FieldDesc("s_phone","string"),
                FieldDesc("s_acctbal","double"),
                FieldDesc("s_comment","string"),
                FieldDesc("n_nationkey","int64"),
                FieldDesc("n_name","string"),
                FieldDesc("r_regionkey","int64")};

        vector<FieldDesc> threeTJBuildOutputSchema = { FieldDesc("s_name","string"),
                                                       FieldDesc("s_address","string"),
                                                       FieldDesc("s_phone","string"),
                                                       FieldDesc("s_acctbal","double"),
                                                       FieldDesc("s_comment","string"),
                                                       FieldDesc("n_nationkey","int64"),
                                                       FieldDesc("n_name","string")};
        vector<int> twoTJprobeOutputChannels = {0,1,2,3};
        vector<int> twoTJprobeHashChannels = {1};
        vector<int> threeTJbuildOutputChannels = {1,2,3,4,5,6,7};
        vector<int> threeTJbuildHashChannels = {0};
        LookupJoinDescriptor ppsLookupJoinDescriptor(twoTJProbeSchema,twoTJprobeHashChannels,twoTJprobeOutputChannels,threeTJBuildSchema,threeTJbuildHashChannels,threeTJbuildOutputChannels,threeTJBuildOutputSchema);
        LookupJoinNode *fiveTJJoin = new LookupJoinNode(UUID::create_uuid(),ppsLookupJoinDescriptor);
        fiveTJJoin->addProbe(twoTJ);
        fiveTJJoin->addBuild(buildExchange);

        return fiveTJJoin;

    }


    LookupJoinNode *createFinalAgg_FiveJoin()
    {

        LookupJoinNode *join = createRegionJoinNationJoinSupplierJoinPartsupp();

        PartialAggregationNode *partialAgg = createPartialAgg();
        partialAgg->addSource(join);

        LocalExchangeNode *buildLocalExchange = new LocalExchangeNode("buildLocalExchange");
        buildLocalExchange->addSource(partialAgg);

        shared_ptr<PartitioningScheme> schemeBuild1 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SINGLE_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange1 = new ExchangeNode("finalAggExchange",ExchangeNode::GATHER,schemeBuild1,buildLocalExchange);

        FinalAggregationNode *finalAgg = createFinalAgg();
        finalAgg->addSource(buildExchange1);


        shared_ptr<PartitioningScheme> schemeBuild2 = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("FIXED_BROADCAST_DISTRIBUTION"),{}));
        ExchangeNode *buildExchange2 = new ExchangeNode("buildExchange888",ExchangeNode::REPLICATE,schemeBuild2,finalAgg);


        LookupJoinNode *fiveTJ = create5TJ();


        vector<FieldDesc> fiveTJProbeSchema = {FieldDesc("ps_partkey","int64"),
                                               FieldDesc("ps_suppkey","int64"),
                                               FieldDesc("ps_supplycost","double"),
                                               FieldDesc("p_mfgr","string"),
                                               FieldDesc("s_name","string"),
                                               FieldDesc("s_address","string"),
                                               FieldDesc("s_phone","string"),
                                               FieldDesc("s_acctbal","double"),
                                               FieldDesc("s_comment","string"),
                                               FieldDesc("n_nationkey","int64"),
                                               FieldDesc("n_name","string")};




        vector<FieldDesc> finalAggBuildSchema = {
                FieldDesc("ps_partkey","int64"),
                FieldDesc("min_ps_supplycost","double")};


        vector<FieldDesc> finalAggBuildOutputSchema = { FieldDesc("min_ps_supplycost","double")};


        vector<int> fiveTJprobeOutputChannels = {0,1,2,3,4,5,6,7,8,9,10};
        vector<int> fiveTJprobeHashChannels = {0};


        vector<int> finalAggbuildOutputChannels = {1};
        vector<int> finalAggbuildHashChannels = {0};

        LookupJoinDescriptor ppsLookupJoinDescriptor(fiveTJProbeSchema,fiveTJprobeHashChannels,fiveTJprobeOutputChannels,finalAggBuildSchema,finalAggbuildHashChannels,finalAggbuildOutputChannels,finalAggBuildOutputSchema);
        LookupJoinNode *allJoin = new LookupJoinNode(UUID::create_uuid(),ppsLookupJoinDescriptor);

        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(fiveTJ);


        allJoin->addProbe(fiveTJ);
        allJoin->addBuild(buildExchange2);

        return allJoin;

    }

    FilterNode *createAllJoinFilter()
    {



        vector<FieldDesc> fieldsIn = {FieldDesc("ps_partkey","int64"),
                                      FieldDesc("ps_suppkey","int64"),
                                      FieldDesc("ps_supplycost","double"),
                                      FieldDesc("p_mfgr","string"),
                                      FieldDesc("s_name","string"),
                                      FieldDesc("s_address","string"),
                                      FieldDesc("s_phone","string"),
                                      FieldDesc("s_acctbal","double"),
                                      FieldDesc("s_comment","string"),
                                      FieldDesc("n_nationkey","int64"),
                                      FieldDesc("n_name","string"),
                                      FieldDesc("min_ps_supplycost","double")};



        Column *ps_supplycost = new Column("0","ps_supplycost","double");
        Column *min_ps_supplycost = new Column("0","min_ps_supplycost","double");


        FunctionCall *costEqualMin = new FunctionCall("0","equal","bool");
        costEqualMin->addChilds({ps_supplycost,min_ps_supplycost});


        FilterDescriptor filterDescriptor(fieldsIn,costEqualMin);
        FilterNode *filterCost = new FilterNode(UUID::create_uuid(),filterDescriptor);
        return filterCost;


    }






    PartialAggregationNode *createPartialAgg()
    {


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"hash_min",/*inputKey=*/"ps_supplycost",/*outputName=*/"min_partial_ps_supplycost")},
                /*groupByKeys=*/{"ps_partkey"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);


        return partialAggregationNode;

    }

    FinalAggregationNode *createFinalAgg()
    {


        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_min",/*inputKey=*/"min_partial_ps_supplycost",/*outputName=*/"min_ps_supplycost")},
                /*groupByKeys=*/{"ps_partkey"});



        FinalAggregationNode *finalAggregationNode = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);


        return finalAggregationNode;

    }

    TableScanNode *createTableScanPartToBuild()
    {
        TableScanNode *tableScanPart = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","part"));

        return tableScanPart;

    }
    PlanNode *createTableScanPartSuppToProbe()
    {
        TableScanNode *tableScanPartsupp = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","partsupp"));

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange1",ExchangeNode::REPARTITION,schemeProbe,tableScanPartsupp);
        return tableScanPartsupp;

    }
    ExchangeNode *createTableScanNationToProbe()
    {
        TableScanNode *tableScanNation = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","nation"));

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange2",ExchangeNode::REPARTITION,schemeProbe,tableScanNation);
        return probeExchange;

    }
    TableScanNode *createTableScanRegionToBuild()
    {
        TableScanNode *tableScanRegion = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","region"));

        return tableScanRegion;

    }

    ExchangeNode *createTableScanSupplierToProbe()
    {
        TableScanNode *tableScanSupplier = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));

        shared_ptr<PartitioningScheme> schemeProbe = make_shared<PartitioningScheme>(Partitioning::create(SystemPartitioningHandle::get("SCALED_SIMPLE_DISTRIBUTION_BUF"),{}));
        ExchangeNode *probeExchange = new ExchangeNode("probeExchange3",ExchangeNode::REPARTITION,schemeProbe,tableScanSupplier);
        return probeExchange;

    }


};


#endif //OLVP_QUERY2_SINGLE_HPP
