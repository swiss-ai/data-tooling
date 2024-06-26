from swiss_ai.readers.curia_vista import RawCuriaVistaReader
from datatrove.pipeline.tokens import TokensCounter, LengthCounter
from swiss_ai.writers.jsonl import SwissAIJsonlWriter
from datatrove.pipeline.readers import JsonlReader
from datatrove.executor.local import LocalPipelineExecutor
from datetime import datetime

now = datetime.now()

if __name__ == '__main__':
    table = 'Transcript'
    trascr_cols = [
        'Text'
    ]

    now = datetime.now()
    batch = now.strftime("%Y_%m_%d_%H_%M_%S")

    pipeline = [
        JsonlReader(
            data_folder=f"/work_space_data/curiavista/{table}", compression='gzip'
        ),
        RawCuriaVistaReader(
            table=table,
            progress=True,
            columns=trascr_cols,
            limit=1500000
        ),
        TokensCounter(tokenizer_name_or_path='t5-small'),
        LengthCounter(),
        SwissAIJsonlWriter(
            output_folder=f"/work_space_data/curiavista/{table}/jsonl_{batch}"
        )
    ]

    exec = LocalPipelineExecutor(
        pipeline=pipeline,
        skip_completed=False,
        tasks=1,
        workers=1, #needs to be 1 since we are not allowed to span their API
        start_method="spawn",
        logging_dir="/work_space_data/curiavista/logging"
    )

    exec.run()