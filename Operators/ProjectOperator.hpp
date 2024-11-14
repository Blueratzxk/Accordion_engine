//
// Created by zxk on 5/16/23.
//

#ifndef OLVP_PROJECTOPERATOR_HPP
#define OLVP_PROJECTOPERATOR_HPP

#include "../Operators/Operator.hpp"

#include "../Descriptor/ProjectDescriptor.hpp"
#include "../ProjectorAndFilter/ExprAstProjector.hpp"


class ProjectOperator:public Operator
{
    int elementsCount;


    bool finished;

    string name = "ProjectOperator";



    std::shared_ptr<DataPage> inputPage = NULL;
    std::shared_ptr<DataPage> outPutPage = NULL;


    std::shared_ptr<ExprAstProjector> projector = NULL;
    std::shared_ptr<arrow::Schema> input_schema;

    std::shared_ptr<arrow::Schema> output_schema;

    std::shared_ptr<arrow::Schema> projector_output_schema;

    ProjectAssignments assignments;



    ProjectAssignments outputAssignments;

    shared_ptr<DriverContext> driverContext;

    int count = 0;
public:
    string getOperatorId() { return this->name; }

    ProjectOperator(shared_ptr<DriverContext> driverContext,ProjectAssignments assignments) {

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
        this->projector = std::make_shared<ExprAstProjector>(inputSchema,selected_exprOperations,outputs);
        this->projector->Parse();



        if(!this->projector->MakePojector())
        {
            spdlog::critical("ProjectOperator create projector failed!");
            this->projector = NULL;

        }
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

    shared_ptr<arrow::RecordBatch> assembleCompletePage(shared_ptr<DataPage> input,shared_ptr<arrow::RecordBatch> projectorPage)
    {
        vector<shared_ptr<arrow::Array>> columns;

        for(int i = 0 ; i < this->output_schema->num_fields() ; i++)
        {
            if(this->outputAssignments.isRawAssignment(i))
            {
                string originFieldName = this->outputAssignments.getAssignments()[i].first.getFieldName();
                int fieldIndex = input->get()->schema()->GetFieldIndex(originFieldName);
                columns.push_back(input->get()->column(fieldIndex));
            }
            else
            {
                string fieldName = this->output_schema->field(i)->name();
                int fieldIndex = projectorPage->schema()->GetFieldIndex(fieldName);
                columns.push_back(projectorPage->column(fieldIndex));
            }
        }

        return arrow::RecordBatch::Make(this->output_schema,columns[0]->length(),columns);

    }

    void assembleExtraColumn()
    {

    }


    void process()
    {
        if(this->projector != NULL)
        {
            shared_ptr<arrow::RecordBatch> page = this->projector->Project(this->inputPage->get(),this->projector_output_schema);

       //     cout << this->projector->getInputSchema()->ToString() << endl;
       //     cout << "------------------------------"<<endl;
        //    cout << this->inputPage->get()->schema()->ToString()<< endl;

            if(page == NULL)
                this->outPutPage = NULL;
            else
                this->outPutPage = make_shared<DataPage>(assembleCompletePage(this->inputPage,page));

        }
        else
        {
            this->outPutPage = make_shared<DataPage>(assembleCompletePage(this->inputPage,NULL));
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


};
#endif //OLVP_PROJECTOPERATOR_HPP
