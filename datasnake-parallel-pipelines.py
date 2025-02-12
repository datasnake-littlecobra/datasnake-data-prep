from prefect import task, flow

@task
def process_gadm():
    print("📌 Processing GADM data...")
    return "GADM Processed"

@task
def process_wof():
    print("📌 Processing WOF data...")
    return "WOF Processed"

@flow
def parallel_pipeline():
    future1 = process_gadm.submit()
    future2 = process_wof.submit()
    print(future1.result(), future2.result())

parallel_pipeline()
