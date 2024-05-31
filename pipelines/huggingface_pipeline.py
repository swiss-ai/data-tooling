"""

"""

import sys, os, re

sys.path.append("../src")
from datatrove.pipeline.readers.huggingface import HuggingFaceDatasetReader
from datatrove.pipeline.tokens import TokensCounter, LengthCounter
from swiss_ai.writers.jsonl import SwissAIJsonlWriter
from datatrove.executor.local import LocalPipelineExecutor

# os.environ["HF_BASE"] = "/work_space_data/hf_cache"
# Don't forget to set the HF_BASE environment variable to a valid path


def find_years(text):
    # Regex pattern to match four-digit numbers that are likely to be years
    # This pattern matches any number from 1900 to 2099
    pattern = r"\b(19[0-9]{2}|20[0-9]{2})\b"

    # Find all matches in the text
    years = re.findall(pattern, text)

    return years


def _multilegal_adapter(data: dict, path: str, id_in_file: int | str):
    years = find_years(data["text"])
    if len(years) > 0:
        # very crude estimation of the year..
        year = max(int(year) for year in years if int(year) <= 2024)
    else:
        year = 2024
    metadata = {
        "language": data["language"],
        "year": year,
        "optional": {"type": data["type"], "jurisdiction": data["jurisdiction"]},
    }

    return {
        "text": data.pop("text", ""),
        "id": f"{path}/{id_in_file}",
        "media": data.pop("media", []),
        "metadata": metadata,
    }


if __name__ == "__main__":
    INPUT_READER = HuggingFaceDatasetReader(
        dataset="joelniklaus/Multi_Legal_Pile",
        dataset_options={
            "split": "train",
            "name": "all_legislation",
            "cache_dir": os.environ["HF_BASE"],
            "trust_remote_code": True,
        },
        progress=True,
        adapter=_multilegal_adapter,
        limit=1000,
    )
    pipeline = [
        INPUT_READER,
        TokensCounter(),
        LengthCounter(),
        SwissAIJsonlWriter(
            output_folder=f'/{os.environ["HF_BASE"]}/multilegal_pile/jsonl'
        ),
    ]

    main_processing_executor = LocalPipelineExecutor(
        pipeline=pipeline,
        tasks=16,
        workers=1,
        start_method="spawn",
        logging_dir=f'/{os.environ["HF_BASE"]}/multilegal_pile/logging',
    )

    main_processing_executor.run()
