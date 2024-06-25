import json
import os, urllib, requests
import contextlib

from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.io import DataFolderLike, get_datafolder
from datatrove.pipeline.writers.disk_base import DiskWriter

from datatrove.data import Document, DocumentsPipeline
from datatrove.pipeline.base import PipelineStep
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.typeshelper import StatHints
from collections import defaultdict

class RobotsFilter(BaseFilter):
    """
    Performs filtering based on whether the robots.txt disallows some urls
    """

    name = "😈 Robots.txt-filter"

    def __init__(
            self,
            robots_writer: DiskWriter = None,
            dissalow_agents = ["*"],
            exclusion_writer: DiskWriter = None
    ):
        super().__init__(exclusion_writer)
        self.domain_to_dissallowed_urls = {}
        self.robots_writer = robots_writer
        self.dissalow_agents = dissalow_agents

    def fetch_robots_txt(self, domain):
        urls = [f"https://{domain}/robots.txt", f"http://{domain}/robots.txt"]
        for url in urls:
            try:
                response = requests.get(url)
            except:
                continue
            if response.status_code == 200:
                return response.text
        return None

    def parse_robots_txt(self, robots_txt):
        disallowed_paths_for_agent = defaultdict(list)
        current_agent = "*"
        for line in robots_txt.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if line.lower().startswith('user-agent:'):
                current_agent = line.split(':', 1)[1].strip().lower()

            if line.strip().lower().startswith('disallow:'):
                path = line.split(':', 1)[1].strip()
                if path:
                    disallowed_paths_for_agent[current_agent].append(path)

        disallowed_paths = []
        for agent in self.dissalow_agents:
            disallowed_paths.extend(disallowed_paths_for_agent.get(agent.lower(), []))

        return disallowed_paths, disallowed_paths_for_agent

    def check_disallowed_urls(self, domain, disallowed_paths):
        parsed_domain = urllib.parse.urlparse(f"http://{domain}")
        disallowed_urls = [f"{parsed_domain.scheme}://{parsed_domain.netloc}{path}" for path in disallowed_paths]
        return disallowed_urls

    def extract_domain(self, url):
        parsed_url = urllib.parse.urlparse(url)
        return parsed_url.netloc

    def is_url_disallowed(self, url, disallowed_paths):
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        for disallowed_path in disallowed_paths:
            if path.startswith(disallowed_path):
                return True
        return False

    def filter(self, document: Document, writer: DiskWriter, rank: int) -> bool | tuple[bool, str]:
        url = document.metadata['optional'].get("url")
        domain = self.extract_domain(url)

        disallowed_paths = self.domain_to_dissallowed_urls.get(domain, None)
        if not disallowed_paths:
            robots_txt = self.fetch_robots_txt(domain)
            if robots_txt:
                disallowed_paths, disallowed_paths_for_agent = self.parse_robots_txt(robots_txt)
                self.domain_to_dissallowed_urls[domain] = disallowed_paths
                rbts_doc = Document(metadata={domain: disallowed_paths_for_agent}, text='', id='')
                writer.write(rbts_doc, rank)
            else:
                disallowed_paths = []
                self.domain_to_dissallowed_urls[domain] = []

        disallowed = self.is_url_disallowed(url, disallowed_paths)

        return not disallowed, f'Disallowed by Robots.txt: {url}'

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        with self.exclusion_writer if self.exclusion_writer else contextlib.nullcontext() as writer, self.robots_writer if self.robots_writer else contextlib.nullcontext() as rbs_writer:
            for doc in data:
                self.stat_update(StatHints.total)
                with self.track_time():
                    filter_result, reason = self.filter(doc, rbs_writer, rank)
                    if filter_result:
                        self.stat_update(StatHints.forwarded)
                        self.update_doc_stats(doc)
                    else:
                        self.stat_update(StatHints.dropped)
                        if reason:
                            self.stat_update(f"dropped_{reason}")
                        if self.exclusion_writer:
                            if reason:
                                doc.metadata["filter_reason"] = reason
                            writer.write(doc, rank)
                        continue
                yield doc
