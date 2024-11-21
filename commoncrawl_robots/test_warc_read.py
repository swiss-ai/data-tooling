
import urllib

from warcio.archiveiterator import ArchiveIterator

if __name__ == "__main__":
    # fullfilename = "CC-MAIN-2024-42/CC-MAIN-20241016061231-20241016091231-00899.warc.gz"
    fullfilename = "/Users/kylematoba/cscs/CC-MAIN-20241003094020-20241003124020-00001.warc.gz"
    with open(fullfilename, 'rb') as stream:
        for record in ArchiveIterator(stream):
            # print(record)
            if record.rec_type == 'response':
                # print(record)
                if record.http_headers.statusline == "200 OK":
                    print(record.rec_headers.get_header('WARC-Target-URI'), record.content_stream().read())
                # elif record.http_headers.statusline == "200 OK":
                #     print(record.content_stream().read())

                # print(record.rec_headers.get_header('WARC-Target-URI'))

    print("done")


