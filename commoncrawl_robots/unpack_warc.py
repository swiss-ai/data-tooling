import urllib
from collections import Counter

from warcio.archiveiterator import ArchiveIterator

if __name__ == "__main__":
    # fullfilename = "CC-MAIN-2024-42/CC-MAIN-20241016061231-20241016091231-00899.warc.gz"
    # fullfilename = "/Users/kylematoba/cscs/CC-MAIN-20241003094020-20241003124020-00001.warc.gz"
    # fullfilename = "/capstor/store/cscs/swissai/a06/datasets_raw/commoncrawl_kyle/CC-MAIN-2021-43/CC-MAIN-20211015192439-20211015222439-00000.warc.gz"
    # fullfilename = "/capstor/store/cscs/swissai/a06/datasets_raw/commoncrawl_kyle/CC-MAIN-2024-38/CC-MAIN-20240907095856-20240907125856-00000.warc.gz"
    fullfilename = "/Users/kylematoba/cscs/CC-MAIN-20241003094020-20241003124020-00001.warc.gz"
    statuses = dict()
    is_ok_dict = dict()

    with open(fullfilename, 'rb') as stream:
        for idx, record in enumerate(ArchiveIterator(stream)):
            # print(record)
            if record.rec_type == 'response':
                # print(record)
                statuses[idx] = record.http_headers.statusline
                is_ok = record.http_headers.statusline.startswith("200")
                is_ok_dict[idx] = is_ok
                if is_ok:
                    print(record.rec_headers.get_header('WARC-Target-URI'), record.content_stream().read())
                else:
                    print("-" * 20)
                    print(record.http_headers.statusline)
                    print("-" * 20)

                # elif record.http_headers.statusline == "200 OK":
                #     print(record.content_stream().read())

                # print(record.rec_headers.get_header('WARC-Target-URI'))
    c = Counter(statuses.values())
    frac_ok = sum(is_ok_dict.values()) / len(is_ok_dict.values())
    print(c)
    print(f"frac ok = {frac_ok}")
    print("done")
