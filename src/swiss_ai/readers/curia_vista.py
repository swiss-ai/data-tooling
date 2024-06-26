import json, lzma, requests
import os.path
import time
from json import JSONDecodeError
from typing import Callable, List

import xml.etree.ElementTree as ET
from tqdm import tqdm
from langdetect import detect
from datatrove.io import DataFolderLike, get_datafolder
from datatrove.pipeline.readers.base import BaseReader, DocumentsPipeline

BASE_URL = 'https://ws.parlament.ch/odata.svc/'

class RawCuriaVistaReader(BaseReader):
    """Dowlonads the data of Curia Vista and parses the XML

    Args:
        data_folder: the data folder to read from. Directory where the swissdox data is stored
        compression: the compression to use (default: "infer")
        limit: limit the number of files to read
        progress: show progress bar
        adapter: function to adapt the data dict from the source to a Document.
            Take as input: data: dict, path: str, id_in_file: int | str
            Return: a dict with at least a "text" key
        text_key: key to use for the text in the default adapter (default: "text"). Ignored if you provide your own `adapter`
        id_key: key to use for the id in the default adapter (default: "id"). Ignored if you provide your own `adapter`
        default_metadata: default metadata to add to all documents
        recursive: if True, will read files recursively in subfolders (default: True)
        glob_pattern: a glob pattern to filter files to read (default: None)
        shuffle_files: shuffle the files within the returned shard. Mostly used for data viz. purposes, do not use
            with dedup blocks
    """

    name = "🐿 CuriaVista Reader"

    def __init__(
        self,
        table: str,
        columns: List[str],
        limit: int = -1,
        progress: bool = False,
        timeout:int = 1,
        text_key: str = "text",
        id_key: str = "ID",
        default_metadata: dict = None,
    ):
        super().__init__(
            limit,
            progress,
            self._curia_vista_adapter,
            text_key,
            id_key,
            default_metadata,
        )

        self.base_url = f'{BASE_URL}{table}'
        self.table = table
        self.columns = columns
        self.timeout = timeout


    def parse_ids(self, id_url):
        indices = set()
        res = requests.get(id_url)
        root = ET.fromstringlist(res.text)
        for child in root:
            if not child.tag.endswith('entry'):
                continue
            idx = child[-1][0][-1].text
            indices.add(int(idx))
        return indices

    def retrieve_single_record_for_id(self, in_id):
        id_url = f"{self.base_url}?$top=20&$filter=ID eq {in_id}"
        res = requests.get(id_url)
        root = ET.fromstringlist(res.text)
        all_data = []
        for child in root:
            if not child.tag.endswith('entry'):
                continue
            entry = {

            }
            properties = child[-1][0]
            for prop in properties:
                tag = prop.tag.split('}')[-1]
                entry[tag] = prop.text
            all_data.append(entry)

        return all_data

    def _curia_vista_adapter(self, data: dict, path: str, id_in_file: int | str):
        text = ''.join([f'<h2>{col}<h2>{data[col]}' for col in self.columns if data[col] is not None])
        opt_meta_data = {k: v for k, v in data.items() if k not in self.columns}

        lang = opt_meta_data['LanguageOfText'].lower() if opt_meta_data['LanguageOfText'] is not None else None
        if lang is None:
            lang = detect(text)

        meta_data = {
            'optional': opt_meta_data,
            'language': lang,
            'year': int(opt_meta_data['MeetingDate'][:4])
        }

        return {
            "text": text,
            "id": data.pop(self.id_key, f"{path}/{id_in_file}"),
            "media": data.pop("media", []),
            "metadata": meta_data,  # remaining data goes into metadata
        }

    def run(self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        processed_ids = set()
        processed_dp = set()
        try:
            for document in data:
                processed_ids.add(document.id)
                dp = f'{document.id}_{document.metadata["language"]}'
                if dp in processed_dp:
                    continue
                processed_dp.add(dp)
                if document.metadata["language"] is None:
                    document.metadata["language"] = detect(document.text)

                yield document
        except:
            print('Noooo')

        print(f'Already processed {len(processed_ids)} Documents')
        if len(processed_ids) > 0:
            last_id = max(processed_ids)
        else:
            last_id = 0

        ids = ['dummy']
        limit = self.limit

        global_count = 0
        while len(ids) > 0 and global_count < limit:
            id_url = f"{self.base_url}?$filter=Language eq 'DE' and ID gt {last_id} &$orderby=ID&$select=ID&$top=100"
            ids = self.parse_ids(id_url)
            ids = ids.difference(processed_ids)
            ids = sorted(ids)

            for nr, entry_id in tqdm(enumerate(ids, start=1)):
                with self.track_time():
                    entries = self.retrieve_single_record_for_id(entry_id)
                    global_count += len(entries)

                    last_id = entry_id
                    if nr % 10 == 0:
                        time.sleep(self.timeout)

                    for data_dict in entries:
                        document = self.get_document_from_dict(data_dict, self.table, entry_id)
                        if not document:
                            continue
                        yield document
                    if global_count >= limit:
                        break
                    processed_ids.add(entry_id)
            time.sleep(60)
