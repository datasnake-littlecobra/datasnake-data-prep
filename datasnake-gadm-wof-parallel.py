@task
def process_gadm():
    print("📌 Processing GADM data...")
    return "GADM Processed"

@task
def store_gadm():
    print("✅ Storing GADM into Cassandra...")
    return "GADM Stored"

@task
def store_wof():
    print("✅ Storing WOF into Cassandra...")
    return "WOF Stored"

@flow
def full_pipeline():
    gadm_result = process_gadm()
    store_gadm.submit(gadm_result)  # Runs in parallel
    store_wof.submit()  # Runs in parallel

full_pipeline()
