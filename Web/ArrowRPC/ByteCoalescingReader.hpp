#ifndef OLVP_BYTECOALESCINGREADER_HPP
#define OLVP_BYTECOALESCINGREADER_HPP

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/table.h>

class ByteCoalescingReader : public arrow::RecordBatchReader {
public:
    explicit ByteCoalescingReader(
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
        int64_t target_bytes = 1024* 1024 * 1024)
        : batches_(std::move(batches)),
          target_bytes_(target_bytes),
          pos_(0) {}

    std::shared_ptr<arrow::Schema> schema() const override {
        if (batches_.empty()) {
            return arrow::schema({});
        }
        return batches_[0]->schema();
    }

    arrow::Status ReadNext(
        std::shared_ptr<arrow::RecordBatch>* out) override {

        if (pos_ >= batches_.size()) {
            *out = nullptr;
            return arrow::Status::OK();
        }

        auto current_schema = batches_[pos_]->schema();

        std::vector<std::shared_ptr<arrow::RecordBatch>> group;
        int64_t accumulated_bytes = 0;

        while (pos_ < batches_.size()) {

            auto& batch = batches_[pos_];

            // schema切换，停止本轮聚合
            if (!batch->schema()->Equals(current_schema)) {
                break;
            }

            int64_t batch_bytes = EstimateBatchSize(batch);

            // 已经有数据，并且加入后超过目标大小
            if (!group.empty() &&
                accumulated_bytes + batch_bytes > target_bytes_) {
                break;
            }

            group.push_back(batch);
            accumulated_bytes += batch_bytes;
            ++pos_;
        }

        if (group.empty()) {
            *out = nullptr;
            return arrow::Status::OK();
        }

        if (group.size() == 1) {
            *out = group[0];
            return arrow::Status::OK();
        }

        ARROW_ASSIGN_OR_RAISE(
            auto table,
            arrow::Table::FromRecordBatches(
                current_schema,
                group));

        string curSchema = current_schema->ToString();

        auto st = TimeCommon::getCurrentTimeStamp();
        ARROW_ASSIGN_OR_RAISE(
            auto merged_batch,
            table->CombineChunksToBatch());

        auto en = TimeCommon::getCurrentTimeStamp();
        spdlog::warn("CombineChunk:"+to_string(accumulated_bytes)+";Time "+to_string(st-en));

        *out = std::move(merged_batch);

        return arrow::Status::OK();
    }

private:
    static int64_t EstimateBatchSize(
        const std::shared_ptr<arrow::RecordBatch>& batch) {

        int64_t total = 0;

        for (int col = 0; col < batch->num_columns(); ++col) {
            auto arr = batch->column(col);

            for (const auto& buffer : arr->data()->buffers) {
                if (buffer) {
                    total += buffer->size();
                }
            }
        }

        return total;
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    int64_t target_bytes_;
    size_t pos_;
};

#endif // OLVP_BYTECOALESCINGREADER_HPP