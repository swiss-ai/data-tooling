from datatrove.executor import LocalPipelineExecutor
from datatrove.pipeline.readers import ParquetReader
from datatrove.pipeline.filters import LambdaFilter
from datatrove.pipeline.writers import JsonlWriter
from swiss_ai.pipeline.formatters.pii_removal import PIIFormatter
from swiss_ai.pipeline.filters.robots_filter import RobotsFilter
from swiss_ai.writers.jsonl import SwissAIJsonlWriter


def _fineweb_adapter(self, data: dict, path: str, id_in_file: int | str):
    year = int(data.get('date', '2024').split('-')[0])

    metadata = {
        "language": data["language"],
        "year": year,
        "token_count": data["token_count"],
        "optional": {
            "url": data["url"],
            "dump": data["dump"],
            "language_score": data["language_score"],
        },
    }

    return {
        "text": data.pop("text", ""),
        "id": f"{path}/{id_in_file}",
        "media": data.pop("media", []),
        "metadata": metadata,
    }

if __name__ == "__main__":

    robots_writer = JsonlWriter(
        '/work_space_data/swiss_ai/logs/hugginface/fineweb/robots',
        adapter=lambda s, x: x.metadata,
        compression=None
    )

    pipeline_exec = LocalPipelineExecutor(
        pipeline=[
            # replace "data/CC-MAIN-2024-10" with "sample/100BT" to use the 100BT sample
            ParquetReader(
                "/work_space_data/hf_cache/hub/datasets--HuggingFaceFW--fineweb-edu/snapshots/",
                limit=10_000,
                adapter=_fineweb_adapter
            ),
            RobotsFilter(robots_writer=robots_writer, dissalow_agents=['*', 'ccbot']),
            SwissAIJsonlWriter("/work_space_data/swiss_ai/hugginface/fineweb")
        ],
        start_method="spawn",
        workers=8,
        logging_dir='/work_space_data/swiss_ai/logs',
        tasks=10
    )

    pipeline_exec.run()