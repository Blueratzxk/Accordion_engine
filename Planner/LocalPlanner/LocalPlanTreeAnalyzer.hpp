//
// Created by zxk on 5/31/23.
//

#ifndef OLVP_LOCALPLANTREEANALYZER_HPP
#define OLVP_LOCALPLANTREEANALYZER_HPP

#include "../../Frontend/Planner/Expression/RowExpressionTree.h"
#include "../../Frontend/PlanNode/PlanNodeTree.hpp"
#include "../../Operators/LogicalOperators/LogicalOperators.h"
#include "LogicalPipelineFactory.hpp"

#include "../../Operators/Join/PartitionedLookUpSourceFactory.hpp"

#include "../../Operators/LocalExchange/LocalExchange.hpp"
#include "../../Execution/Buffer/OutputBuffer.hpp"
#include "../../Operators/Join/NestedLoopJoin/NestedLoopJoinPagesSupplier.hpp"
class PhysicalOperation
{

    vector<shared_ptr<LogicalOperator>> logicalOperators;
    vector<int> pipelineTypes;
    string maybeSourceId = "NULL";

public:
    PhysicalOperation(shared_ptr<LogicalOperator> lop,PhysicalOperation *source){

        for(int i = 0 ; i < source->getLogicalOperators().size() ; i++)
            this->logicalOperators.push_back(source->getLogicalOperators()[i]);
        this->logicalOperators.push_back(lop);
        for(int i = 0 ; i < source->getPipelineTypes().size() ; i++)
        {
            this->pipelineTypes.push_back(source->getPipelineTypes()[i]);
        }
        this->maybeSourceId = source->maybeSourceId;

        delete(source);
    }
    void setSourceId(string id)
    {
        this->maybeSourceId = id;
    }
    string getSourceId()
    {
        return this->maybeSourceId;
    }
    bool hasSourceId()
    {
        if(this->maybeSourceId.compare("NULL") == 0)
            return false;
        else
            return true;
    }
    void clearSourceId()
    {
        this->maybeSourceId = "NULL";
    }
    PhysicalOperation(shared_ptr<LogicalOperator> lop){

        this->logicalOperators.push_back(lop);
    }
    vector<shared_ptr<LogicalOperator>> getLogicalOperators()
    {
        return this->logicalOperators;
    }
    void addType(int type)
    {
        this->pipelineTypes.push_back(type);
    }
    void addOperator(shared_ptr<LogicalOperator> insert)
    {
        this->logicalOperators.push_back(insert);
    }
    vector<int> getPipelineTypes()
    {
        return this->pipelineTypes;
    }


};

class LocalExecutionPlanContext
{
    vector<LogicalPipeline> DriverFactories;
    int nextPipelineId;
    int nextOperatorId;
    string localExchangeType = "None";
    string downStreamProbeOrBuild = "None";
    int joinNums = 0;
public:
    LocalExecutionPlanContext(){
        this->nextPipelineId = 0;
    }
    void addLogicalPipeline(LogicalPipeline lpl)
    {
        this->DriverFactories.push_back(lpl);
    }
    string getNextId()
    {
        string Id = to_string(nextPipelineId);
        nextPipelineId++;
        return Id;
    }
    void addJoinNum()
    {
        this->joinNums ++;
    }
    int getJoinNum()
    {
        return this->joinNums;
    }
    string getNextOperatorId()
    {
        string Id = to_string(nextOperatorId);
        nextOperatorId++;
        return Id;
    }
    vector<LogicalPipeline> getDriverFactories()
    {
        return this->DriverFactories;
    }

    void setDownstreamProbeOrBuild(string type)
    {
        this->downStreamProbeOrBuild = type;
    }
    string getDownstreamProbeOrBuild()
    {
        return this->downStreamProbeOrBuild;
    }
    void setLocalExchangeType(string type)
    {
        this->localExchangeType = type;
    }
    string getLocalExchangeType()
    {
        return this->localExchangeType;
    }





};







//这里主要是实现生成logical operator，以及生成相应的连接结构，解析localexchange和join会生成pipeline
class LocalPlanNodeTreeBreaker : public NodeVisitor {

private:

public:
    LocalPlanNodeTreeBreaker() {

    }

    void* VisitExchangeNode(ExchangeNode* node,void *context) override{

        cout << "exchangNode shouldn't show up here!"<<endl;
        exit(0);
        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);
        return source;
    }
    void* VisitFilterNode(FilterNode* node,void *context)override{

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

         auto filter = std::make_shared<Logical_FilterOperator>(node->getFilterDesc());

        return  new PhysicalOperation(filter,source);
    }

    void* VisitFinalAggregationNode(FinalAggregationNode* node,void *context) override{

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

        auto fagg = std::make_shared<Logical_FinalAggregationOperator>(node->getAggregationDesc());

        return new PhysicalOperation(fagg,source);
    }
    void* VisitLookupJoinNode(LookupJoinNode* node,void *context) override{

        shared_ptr<arrow::Schema> buildInputSchema = node->getLookupJoinDescriptor().getBuildInputArrowSchema();
        shared_ptr<arrow::Schema> buildOutputSchema = node->getLookupJoinDescriptor().getBuildOutputArrowSchema();
        shared_ptr<arrow::Schema> probeInputSchema = node->getLookupJoinDescriptor().getProbeInputArrowSchema();

        ExecutionConfig config;
        int hashBuildCon = atoi(config.getIntra_task_hash_build_concurrency().c_str());
        if((hashBuildCon & hashBuildCon - 1) != 0)
        {
            spdlog::warn("The value of intra-task hash build concurrency must be an integer power of 2!");
            hashBuildCon = 1;
        }
        std::shared_ptr<PartitionedLookupSourceFactory>  lookupSourceFactory = std::make_shared<PartitionedLookupSourceFactory>(buildInputSchema,buildOutputSchema,hashBuildCon);

        ((LocalExecutionPlanContext*)context)->setDownstreamProbeOrBuild("build");
        ((LocalExecutionPlanContext*)context)->addJoinNum();

        PhysicalOperation *sourceBuild = (PhysicalOperation*)Visit(node->getBuild(),context);
        auto build = make_shared<Logical_HashBuilderOperator>(node->getId(),lookupSourceFactory,node->getLookupJoinDescriptor().getBuildOutputChannels(),node->getLookupJoinDescriptor().getBuildHashChannels());
        PhysicalOperation *buildPipeline = new PhysicalOperation(build,sourceBuild);



        string id = ((LocalExecutionPlanContext*)context)->getNextId();
        string buildLocalExchangeType = ((LocalExecutionPlanContext*)context)->getLocalExchangeType();
        ((LocalExecutionPlanContext*)context)->setLocalExchangeType("None");
        if(buildLocalExchangeType != "hash")
            lookupSourceFactory->resetPartitionCount();


        LogicalPipeline lp = LogicalPipeline(id,buildPipeline->getPipelineTypes(),buildPipeline->getLogicalOperators());
        if(buildPipeline->hasSourceId()) {
            lp.setSourceId(buildPipeline->getSourceId());
            buildPipeline->clearSourceId();
        }
        ((LocalExecutionPlanContext*)context)->addLogicalPipeline(lp);






        ((LocalExecutionPlanContext*)context)->setDownstreamProbeOrBuild("probe");
        PhysicalOperation *sourceProbe = (PhysicalOperation*)Visit(node->getProbe(),context);


        shared_ptr<JoinProbeFactory> joinProbeFactory = std::make_shared<JoinProbeFactory>(node->getLookupJoinDescriptor().getProbeOutputChannels()
                ,node->getLookupJoinDescriptor().getProbeHashChannels());

        auto hashProbe = make_shared<Logical_LookupJoinOperator>(probeInputSchema,buildOutputSchema,joinProbeFactory,lookupSourceFactory);



        return new PhysicalOperation(hashProbe,sourceProbe);

    }


    void* VisitCrossJoinNode(CrossJoinNode* node,void *context) override{

        ((LocalExecutionPlanContext*)context)->addJoinNum();
        shared_ptr<arrow::Schema> probeInputSchema = node->getCrossJoinDescriptor().getProbeInputArrowSchema();
        shared_ptr<arrow::Schema> buildInputSchema = node->getCrossJoinDescriptor().getBuildInputArrowSchema();
        shared_ptr<arrow::Schema> probeOutputSchema = node->getCrossJoinDescriptor().getProbeOutputArrowSchema();
        shared_ptr<arrow::Schema> buildOutputSchema = node->getCrossJoinDescriptor().getBuildOutputArrowSchema();

        std::shared_ptr<NestedLoopJoinPagesSupplier>  supplier = std::make_shared<NestedLoopJoinPagesSupplier>();



        ((LocalExecutionPlanContext*)context)->setDownstreamProbeOrBuild("build");
        PhysicalOperation *sourceBuild = (PhysicalOperation*)Visit(node->getBuild(),context);

        auto build = make_shared<Logical_NestedLoopBuildOperator>(node->getId(),supplier);
        PhysicalOperation *buildPipeline = new PhysicalOperation(build,sourceBuild);

        string id = ((LocalExecutionPlanContext*)context)->getNextId();


        LogicalPipeline lp = LogicalPipeline(id,buildPipeline->getPipelineTypes(),buildPipeline->getLogicalOperators());
        if(buildPipeline->hasSourceId()) {
            lp.setSourceId(buildPipeline->getSourceId());
            buildPipeline->clearSourceId();
        }
        ((LocalExecutionPlanContext*)context)->addLogicalPipeline(lp);


        ((LocalExecutionPlanContext*)context)->setDownstreamProbeOrBuild("probe");
        PhysicalOperation *sourceProbe = (PhysicalOperation*)Visit(node->getProbe(),context);



        auto probe = make_shared<Logical_NestedLoopJoinOperator>(probeInputSchema,buildInputSchema,probeOutputSchema,buildOutputSchema,supplier);



        return new PhysicalOperation(probe,sourceProbe);

    }
    void* VisitSortNode(SortNode* node,void *context) override{

        PhysicalOperation *source = (PhysicalOperation*)Visit(node->getSource(),context);

        auto sort = make_shared<Logical_SortOperator>(node->getSortDesc().getSortKeys(),node->getSortDesc().getSortOrders());

        return new PhysicalOperation(sort,source);
    }
    void* VisitTaskOutputNode(TaskOutputNode* node,void *context) override{

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

        auto output = make_shared<Logical_TaskOutputOperator>();

        return new PhysicalOperation(output,source);
    }

    void* VisitPartialAggregationNode(PartialAggregationNode* node,void *context) override{

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

        auto pagg = make_shared<Logical_PartialAggregationOperator>(node->getAggregationDesc());


        return new PhysicalOperation(pagg,source);
    }

    void* VisitProjectNode(ProjectNode* node,void *context)override {

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

        auto proj = make_shared<Logical_ProjectOperator>(node->getProjectAssignments());

        return new PhysicalOperation(proj,source);
    }

    void* VisitLimitNode(LimitNode* node,void *context)override {

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

        auto limit = make_shared<Logical_LimitOperator>(node->getLimit());

        return new PhysicalOperation(limit,source);
    }


    void* VisitRemoteSourceNode(RemoteSourceNode* node,void *context) override {

        string probeOrBuild = ((LocalExecutionPlanContext *) context)->getDownstreamProbeOrBuild();

        auto remote = make_shared<Logical_RemoteSourceOperator>(probeOrBuild);
        PhysicalOperation *re = new PhysicalOperation(remote);
        re->addType(PIPELINE_TYPE_REMOTESOURCE);
        re->setSourceId(node->getId());
        return re;

    }
    void* VisitTableScanNode(TableScanNode* node,void *context) override{


        auto PSM = std::make_shared<PageSourceManager>();
        ConnectorId connectorId("tpch_test");
        std::shared_ptr<SystemPageSourceProvider> sys = std::make_shared<SystemPageSourceProvider>();
        PSM->addConnectorPageSourceProvider(connectorId, sys);



        auto tableScan = make_shared<Logical_TableScanOperator>(node->getId(),PSM);

        PhysicalOperation *po = new PhysicalOperation(tableScan);

        po->addType(PIPELINE_TYPE_TABLESCAN);
        po->setSourceId(node->getId());
        return po;
    }
    void* VisitTopKNode(TopKNode* node,void *context) override{

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

        auto topk = make_shared<Logical_TopKOperator>(node->getTopKDescriptor().getK(),node->getTopKDescriptor().getSortKeysTopk(),node->getTopKDescriptor().getSortOrdersTopKey());

        return new PhysicalOperation(topk,source);

    }
    void* VisitLocalExchangeNode(LocalExchangeNode* node,void *context) override{

        PhysicalOperation *source = (PhysicalOperation *)Visit(node->getSource(),context);

        std::shared_ptr<LocalExchangeFactory> localExchangeFactory = std::make_shared<LocalExchangeFactory>(1,1,node->getExchangeType(),node->gethashExchangeChannels());

        auto sink = std::make_shared<Logical_LocalExchangeSinkOperator>(localExchangeFactory);

        PhysicalOperation *sinkPipeline = new PhysicalOperation(sink,source);
        sinkPipeline->addType(PIPELINE_TYPE_SINK);

        string id = ((LocalExecutionPlanContext*)context)->getNextId();

        LogicalPipeline lp = LogicalPipeline(id,sinkPipeline->getPipelineTypes(),sinkPipeline->getLogicalOperators());
        if(sinkPipeline->hasSourceId()) {
            lp.setSourceId(sinkPipeline->getSourceId());
            sinkPipeline->clearSourceId();
        }
        ((LocalExecutionPlanContext*)context)->addLogicalPipeline(lp);

        ((LocalExecutionPlanContext*)context)->setLocalExchangeType(node->getExchangeType());


        auto sourceOp = std::make_shared<Logical_LocalExchangeSourceOperator>(localExchangeFactory);


        PhysicalOperation *po = new PhysicalOperation(sourceOp);
        po->addType(PIPELINE_TYPE_SOURCE);


        return po;
    }


    void * VisitAggregationNode(AggregationNode *node, void *context) override
    {
        return NULL;
    }

};


class LocalExecutionPlan
{
    list<LogicalPipeline> driverFactories;
public:
    LocalExecutionPlan(list<LogicalPipeline> driverFactories)
    {
        this->driverFactories = driverFactories;
    }

    list<LogicalPipeline> getDriverFactories()
    {
        return this->driverFactories;
    }

};


class LocalExecutionPlanner {

    LocalExecutionPlanContext *context;
    LocalPlanNodeTreeBreaker visitor;
    PlanNode *root;
    std::shared_ptr<OutputBuffer> buffer = NULL;

public:
    LocalExecutionPlanner(PlanNode *root) {
        this->root = root;
        this->context = new LocalExecutionPlanContext();
    }

    LocalExecutionPlanner(PlanNode *root, shared_ptr<OutputBuffer> buffer) {
        this->root = root;
        this->context = new LocalExecutionPlanContext();
        this->buffer = buffer;

        if (buffer != NULL) {
            string id = root->getType();
            if (id.compare("TaskOutputNode") == 0) {
                PlanNode *temp = root;
                this->root = root->getSource();
                delete temp;
            }
        }
    }


    LocalExecutionPlanner() {
        root = getAtestTree3();
        this->context = new LocalExecutionPlanContext();
    }

    ~LocalExecutionPlanner() {
        // NodeCleanVisitor cleaner;
        //  cleaner.Visit(this->root,NULL);
    }

    PlanNode *getRoot() {
        return this->root;
    }

    void analyze() {
        PhysicalOperation *op = (PhysicalOperation *) visitor.Visit(root, context);

        if (this->buffer != NULL) {
            auto output = make_shared<Logical_TaskOutputOperator>(buffer);
            op->addOperator(output);

        }
        op->addType(PIPELINE_TYPE_OUTPUT);
        string Id = context->getNextId();

        LogicalPipeline lp = LogicalPipeline(Id, op->getPipelineTypes(), op->getLogicalOperators());
        if (op->hasSourceId()) {
            lp.setSourceId(op->getSourceId());
            op->clearSourceId();
        }
        context->addLogicalPipeline(lp);
        delete (op);

    }

    std::shared_ptr<LocalExecutionPlan> plan()
    {
        this->analyze();

        vector<LogicalPipeline> lps = this->context->getDriverFactories();

        list<LogicalPipeline> llps;

        for(int i = 0 ; i < lps.size() ; i++)
            llps.push_back(lps[i]);

        return std::make_shared<LocalExecutionPlan>(llps);
    }



    LocalExecutionPlanContext *getContext() {
        return this->context;
    }



    PlanNode* getAtestTree3()
    {

        TableScanNode *tableScanProbe = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));
        TableScanNode *tableScanBuild = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));





        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(tableScanProbe);




        vector<FieldDesc> inputProbeSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_address","string"),FieldDesc("s_nationkey","int64"),FieldDesc("s_phone","string"),FieldDesc("s_acctbal","double"),FieldDesc("s_comment","string")};
        vector<FieldDesc> inputBuildSchema = {FieldDesc("s_suppkey2","int64"),FieldDesc("s_name2","string"),FieldDesc("s_address2","string"),FieldDesc("s_nationkey2","int64"),FieldDesc("s_phone2","string"),FieldDesc("s_acctbal2","double"),FieldDesc("s_comment2","string")};
        vector<FieldDesc> outputSchema = {FieldDesc("s_suppkey2","int64"),FieldDesc("s_nationkey2","int64")};
        vector<int> tprobeOutputChannels = {0,3};
        vector<int> tprobeHashChannels = {3};
        vector<int> tbuildOutputChannels = {0,3};
        vector<int> tbuildHashChannels = {3};
        LookupJoinDescriptor lookupJoinDescriptor(inputProbeSchema,tprobeHashChannels,tprobeOutputChannels,inputBuildSchema,tbuildHashChannels,tbuildOutputChannels,outputSchema);
        LookupJoinNode *lookupJoin = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);
        lookupJoin->addBuild(tableScanBuild);
        lookupJoin->addProbe(probeLocalExchange);


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"s_nationkey",/*outputName=*/"s_nationkey_all"),
                                        AggregateDesc(/*functionName=*/"hash_count",/*inputKey=*/"s_nationkey",/*outputName=*/"s_nationkey_count")
                                       },
                /*groupByKeys=*/{"s_nationkey"});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);
        partialAggregationNode->addSource(lookupJoin);



        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(partialAggregationNode);






        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"s_nationkey_all",/*outputName=*/"s_nationkey_alll"),
                                      AggregateDesc(/*functionName=*/"hash_sum",/*inputKey=*/"s_nationkey_count",/*outputName=*/"s_nationkey_countt")
                                     },
                /*groupByKeys=*/{"s_nationkey"});

        FinalAggregationNode *fagg = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);
        fagg->addSource(paggLocalExchange);




        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(fagg);
        return (PlanNode*)output;

    }



    PlanNode* getAtestTree4()
    {

        TableScanNode *tableScanProbe = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));
        TableScanNode *tableScanBuild = new TableScanNode(UUID::create_uuid(),TableScanDescriptor("tpch_test","tpch_1","supplier"));





        LocalExchangeNode *probeLocalExchange = new LocalExchangeNode("probeLocalExchange");
        probeLocalExchange->addSource(tableScanProbe);




        vector<FieldDesc> inputProbeSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_address","string"),FieldDesc("s_nationkey","int64"),FieldDesc("s_phone","string"),FieldDesc("s_acctbal","double"),FieldDesc("s_comment","string")};
        vector<FieldDesc> inputBuildSchema = {FieldDesc("s_suppkey2","int64"),FieldDesc("s_name2","string"),FieldDesc("s_address2","string"),FieldDesc("s_nationkey2","int64"),FieldDesc("s_phone2","string"),FieldDesc("s_acctbal2","double"),FieldDesc("s_comment2","string")};
        vector<FieldDesc> outputSchema = {FieldDesc("s_suppkey","int64"),FieldDesc("s_name","string"),FieldDesc("s_nationkey","int64")};
        vector<int> tprobeOutputChannels = {0,3};
        vector<int> tprobeHashChannels = {3};
        vector<int> tbuildOutputChannels = {0,3};
        vector<int> tbuildHashChannels = {3};
        LookupJoinDescriptor lookupJoinDescriptor(inputProbeSchema,tprobeHashChannels,tprobeOutputChannels,inputBuildSchema,tbuildOutputChannels,tbuildHashChannels,outputSchema);
        LookupJoinNode *lookupJoin = new LookupJoinNode(UUID::create_uuid(),lookupJoinDescriptor);
        lookupJoin->addBuild(tableScanBuild);
        lookupJoin->addProbe(probeLocalExchange);


        AggregationDesc PartialAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"s_suppkey",/*outputName=*/"s_nationkey_all"),
                                        AggregateDesc(/*functionName=*/"count",/*inputKey=*/"s_suppkey",/*outputName=*/"s_nationkey_count")
                                       },
                /*groupByKeys=*/{});

        PartialAggregationNode *partialAggregationNode = new PartialAggregationNode(UUID::create_uuid(),PartialAggDesc);
        partialAggregationNode->addSource(lookupJoin);



        LocalExchangeNode *paggLocalExchange = new LocalExchangeNode("paggLocalExchange");
        paggLocalExchange->addSource(partialAggregationNode);






        AggregationDesc FinalAggDesc({AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"s_nationkey_all",/*outputName=*/"s_nationkey_alll"),
                                      AggregateDesc(/*functionName=*/"sum",/*inputKey=*/"s_nationkey_count",/*outputName=*/"s_nationkey_countt")
                                     },
                /*groupByKeys=*/{});

        FinalAggregationNode *fagg = new FinalAggregationNode(UUID::create_uuid(),FinalAggDesc);
        fagg->addSource(paggLocalExchange);




        TaskOutputNode *output = new TaskOutputNode(UUID::create_uuid());

        output->addSource(fagg);
        return (PlanNode*)output;

    }

};


#endif //OLVP_LOCALPLANTREEANALYZER_HPP
