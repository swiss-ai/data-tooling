import os

if __name__ == "__main__":
    base_dir = "/capstor/store/cscs/swissai/a06/datasets_raw/commoncrawl_kyle"
    # ident = "CC-MAIN-2024-38"
    idents = [
              # "CC-MAIN-2024-33", "CC-MAIN-2024-30",
              # "CC-MAIN-2024-26", "CC-MAIN-2024-22",
              "CC-MAIN-2024-18", "CC-MAIN-2024-10",
              "CC-MAIN-2023-50", "CC-MAIN-2023-40",
              "CC-MAIN-2023-23_small", "CC-MAIN-2023-14", "CC-MAIN-2023-06", "CC-MAIN-2022-49",
              "CC-MAIN-2022-40", "CC-MAIN-2022-33", "CC-MAIN-2022-27", "CC-MAIN-2022-21",
              "CC-MAIN-2022-05", "CC-MAIN-2021-49", "CC-MAIN-2021-43"]
    for idx, ident in enumerate(idents):
        # idx = 0; ident = idents[idx]
        # idx = 1; ident = idents[idx]
        path_to_create = os.path.join(base_dir, ident)
        os.path.makedirs(path_to_create, exist_ok=True)

        os.chdir(path_to_create)
        parallel_processes = 100
        system_call = f"aws s3 ls s3://commoncrawl/crawl-data/{ident}/segments/ --recursive | awk '{{print $4}}' | grep 'robotstxt/.*\.warc\.gz$' | xargs -P {parallel_processes} -I {{}} aws s3 cp s3://commoncrawl/{{}} ."
        print(system_call)
        os.system(system_call)

        os.system(f"tar -zcvf {ident}.tar.gz {ident}")

    """
    
    
    
    tar -cvf myfolder.tar myfolder
    """