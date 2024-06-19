from datatrove.executor import LocalPipelineExecutor
from datatrove.pipeline.readers import ParquetReader
from datatrove.pipeline.filters import LambdaFilter
from datatrove.pipeline.writers import JsonlWriter
from swiss_ai.pipeline.formatters.pii_removal import PIIFormatter

if __name__ == "__main__":
    pipeline_exec = LocalPipelineExecutor(
        pipeline=[
            # replace "data/CC-MAIN-2024-10" with "sample/100BT" to use the 100BT sample
            ParquetReader("/work_space_data/hf_cache/hub/datasets--HuggingFaceFW--fineweb-edu/snapshots/", limit=10_000),
            PIIFormatter(),
            JsonlWriter("/work_space_data/swiss_ai/hugginface/fineweb")
        ],
        start_method="spawn",
        workers=8,
        logging_dir='work_space_data/swiss_ai/logs',
        tasks=10
    )

    pipeline_exec.run()