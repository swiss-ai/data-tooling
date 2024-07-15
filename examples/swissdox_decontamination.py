""" Example (work in progress) for decontaminating Swissdox from FineWeb.

Steps to run this example:
```
git clone https://github.com/swiss-ai/data-tooling.git
cd data-tooling
git checkout decont
pip install -e .
pip install lighteval
python swissdox_decontamination.py
```

TODO: Need to bring protected data into right format.
NGramsDecontIndexer expects a 'query' field.
"""

from datatrove.executor import LocalPipelineExecutor
from datatrove.pipeline.decont.n_grams import (
    NGramsDecontConfig,
    NGramsDecontIndexer,
    NGramsDecontFilter,
)
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.readers import ParquetReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers import JsonlWriter
from datatrove.utils.typeshelper import Languages


DATA_BASE_PATH = "/store/swissai/a06/datasets"
WORKSPACE = "workspace/swissai"

NUM_WORKERS = 16
NUM_TASKS = 16

### DATA ###
# protected data that we want to remove in other datasets
PROTECTED_READER = JsonlReader(
    f"{DATA_BASE_PATH}/swissdox/data",
    compression="gzip",
    limit=2,
    doc_progress=True,
    file_progress=True,
)

# training data that we want to decontaminate
def _fineweb_adapter(self, data: dict, path: str, id_in_file: int | str):
    year = int(data.get("date", "2024").split("-")[0])
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


DATA_READER = ParquetReader(
    f"{DATA_BASE_PATH}/fineweb-edu-sample-10BT/sample/10BT",
    limit=10,
    adapter=_fineweb_adapter,
    doc_progress=True,
    file_progress=True,
)

### DECONTAMINATION ###
DECONT_CONFIG = NGramsDecontConfig(n_grams=12)

DECONT_INDEXER = NGramsDecontIndexer(
    output_folder=f"{WORKSPACE}/decont_index",
    config=DECONT_CONFIG,
    language=Languages.english,
)

DECONT_FILTER = NGramsDecontFilter(
    index_folder=f"{WORKSPACE}/decont_index",
    config=DECONT_CONFIG,
    exclusion_writer=JsonlWriter(f"{DATA_BASE_PATH}/decont/contaminated-fineweb"),
    language=Languages.english,
)

### OUTPUT ###
OUTPUT_WRITER = JsonlWriter(f"{DATA_BASE_PATH}/decont/decontaminated-fineweb")


def main():
    pipeline_index = LocalPipelineExecutor(
        pipeline=[
            PROTECTED_READER,
            DECONT_INDEXER,
        ],
        start_method="spawn",
        workers=1,  # required by NGramsDecontIndexer
        logging_dir=f"{WORKSPACE}/logs_index",
        tasks=1,  # required by NGramsDecontIndexer
    )
    pipeline_filter = LocalPipelineExecutor(
        pipeline=[
            DATA_READER,
            TokensCounter(),
            DECONT_FILTER,
            OUTPUT_WRITER,
        ],
        start_method="spawn",
        workers=NUM_WORKERS,
        logging_dir=f"{WORKSPACE}/logs_filter",
        tasks=NUM_TASKS,
    )
    pipeline_index.run()
    pipeline_filter.run()


if __name__ == "__main__":
    main()
