//
// Created by zxk on 8/17/25.
//

#ifndef OLVP_GPUPROJECTOPERATOR_HPP
#define OLVP_GPUPROJECTOPERATOR_HPP





#include "../Operators/Operator.hpp"

#include "../Descriptor/ProjectDescriptor.hpp"
#include "../../Execution/Task/Context/DriverContext.h"
#include "GPUProjector.hpp"

class GPUProjectOperator:public Operator
{
    int elementsCount;


    bool finished;

    string name = "GPUProjectOperator";



    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;



    std::shared_ptr<arrow::Schema> input_schema;

    std::shared_ptr<arrow::Schema> output_schema;

    std::shared_ptr<arrow::Schema> projector_output_schema;

    ProjectAssignments assignments;

    shared_ptr<GPUProjector> projector;


    ProjectAssignments outputAssignments;

    GPUFunctions gpuFunctions;

    shared_ptr<DriverContext> driverContext;

    int count = 0;
public:
    string getOperatorId() { return this->name; }

    GPUProjectOperator(shared_ptr<DriverContext> driverContext,ProjectAssignments assignments) {

        this->assignments = assignments;

        this->elementsCount = 0;
        this->finished = false;
        this->driverContext = driverContext;



        this->generateInputSchema();
        this->makeProjector(this->input_schema);
        this->generateOutputSchema();


    }

    void addInput(std::shared_ptr<DataPage> input) override {
        if(input != NULL && input->getElementsCount() != 0) {
            this->inputPage = input;
            this->count++;
        }

    }

    void makeProjector(shared_ptr<arrow::Schema> inputSchema)
    {

        std::vector<std::shared_ptr<arrow::Field>> arrowOutputFields;
        vector<ExprOutput> outputs;


        vector<pair<FieldDesc,FieldDesc>> selected_assignments;
        vector<std::shared_ptr<AstNodePtr>> selected_exprOperations;




        if(!assignments.getComputationProjections(selected_assignments,selected_exprOperations))
            return;
        for(int i = 0 ; i < selected_assignments.size() ; i++) {

            string outputFName = selected_assignments[i].second.getFieldName();
            string outputFType = selected_assignments[i].second.getFieldType();

            ExprOutput output(outputFName, outputFType);
            outputs.push_back(output);
            arrowOutputFields.push_back(arrow::field(outputFName, Typer::getType(outputFType)));
        }

        if(selected_assignments.empty())
        {
            this->projector_output_schema = NULL;
            this->projector = NULL;
            return;
        }

        this->projector_output_schema = arrow::schema(arrowOutputFields);
        this->projector = std::make_shared<GPUProjector>(inputSchema,selected_exprOperations,outputs);

    }



    void generateInputSchema()
    {
        std::vector<std::shared_ptr<arrow::Field>> arrowInputFields;

        vector<pair<FieldDesc,FieldDesc>> selected_assignments;
        vector<std::shared_ptr<AstNodePtr>> selected_exprOperations;
        if(!this->assignments.getProjections(selected_assignments,selected_exprOperations))
            return;

        set<string> uniqueFields;
        for(int i = 0 ; i < selected_assignments.size() ; i++)
        {
            string inputFName = selected_assignments[i].first.getFieldName();
            string inputFType = selected_assignments[i].first.getFieldType();

            if(uniqueFields.count(inputFName) == 0)
            {
                arrowInputFields.push_back(arrow::field(inputFName,Typer::getType(inputFType)));
                uniqueFields.insert(inputFName);
            }

        }
        this->input_schema = arrow::schema(arrowInputFields);
    }

    void generateOutputSchema()
    {
        vector<pair<FieldDesc,FieldDesc>> selected_assignments;
        vector<std::shared_ptr<AstNodePtr>> selected_exprOperations;
        std::vector<std::shared_ptr<arrow::Field>> projectOperatorOutputFields;
        if(!this->assignments.getReserveProjections(selected_assignments,selected_exprOperations))
            return;

        for(int i = 0 ; i < selected_assignments.size() ; i++) {

            string outputFName = selected_assignments[i].second.getFieldName();
            string outputFType = selected_assignments[i].second.getFieldType();
            projectOperatorOutputFields.push_back(arrow::field(outputFName, Typer::getType(outputFType)));
        }
        this->output_schema = arrow::schema(projectOperatorOutputFields);
        this->outputAssignments = ProjectAssignments(selected_assignments,selected_exprOperations);
    }

    shared_ptr<DataPage> assembleCompletePage(shared_ptr<DataPage> input,vector<void*> projectedColumns)
    {
        vector<void*> columns;

        for(int i = 0 ; i < this->output_schema->num_fields() ; i++)
        {
            if(this->outputAssignments.isRawAssignment(i))
            {
                string originFieldName = this->outputAssignments.getAssignments()[i].first.getFieldName();
                int fieldIndex = input->get()->schema()->GetFieldIndex(originFieldName);

                if(fieldIndex == -1)
                    spdlog::error(input->get()->schema()->ToString());


                columns.push_back(gpuFunctions.cudfCopyColumnByIndex(input->getExtensionPage(),fieldIndex));
            }
            else
            {
                string fieldName = this->output_schema->field(i)->name();
                int fieldIndex = this->projector_output_schema->GetFieldIndex(fieldName);
                columns.push_back(projectedColumns[fieldIndex]);
            }
        }

        auto newCudfTable = gpuFunctions.cudfMakeTable(columns);
        return make_shared<DataPage>(newCudfTable,output_schema,input->getElementsCount(),DataPage::GPU);
    }

    void assembleExtraColumn()
    {

    }


    void process()
    {
        if(this->projector != NULL)
        {
            vector<void*> projectResults = this->projector->evaluate(this->inputPage);


            this->outPutPage = assembleCompletePage(this->inputPage,projectResults);

        }
        else
        {
            this->outPutPage = assembleCompletePage(this->inputPage, {});
        }
    }


    std::shared_ptr<DataPage> getOutput() override {


        if(this->inputPage == NULL)
            return NULL;

        if(this->inputPage->isEndPage()) {
            spdlog::debug("project process "+ to_string(this->count)+" pages");
            this->finished = true;
        }

        if(this->finished)
        {
            this->outPutPage = this->inputPage;
        }
        else
        {
            process();
        }

        gpuFunctions.freeGPUPage(this->inputPage->getExtensionPage());
        this->inputPage = NULL;

        return this->outPutPage;

    }


    bool needsInput() override {
        if(this->inputPage == NULL)
            return true;
        else
            return false;
    }


    bool isFinished()
    {
        return this->finished;
    }

    int isExtension() override
    {
        return true;
    }
    shared_ptr<DataPage> downloadToCPU(shared_ptr<DataPage> page) override
    {
        if(page->isEndPage())
            return page;
        auto table = gpuFunctions.getCPUPageFromGPU(page->getExtensionPage(),this->output_schema);
        return make_shared<DataPage>(table->CombineChunksToBatch().ValueOrDie());
    }
    shared_ptr<DataPage> uploadToExtension(shared_ptr<DataPage> page) override
    {
        spdlog::info("project uploadToExtension:"+ to_string(page->getElementsCount()));
        if(page->isEndPage())
            return page;
        return make_shared<DataPage>(gpuFunctions.pushCPUPageToGPU(page->get()),page->get()->schema(),page->getElementsCount(),DataPage::GPU);
    }


};

#endif //OLVP_GPUPROJECTOPERATOR_HPP
