import pyodbc
import datetime
import logging
import json
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient

BATCH_SIZE = 1000  # process 1000 rows at a time

def main(timer: func.TimerRequest):

    logging.info("ArchiveOrders Function Started")

    sql_conn_str = os.getenv("SQL_CONNECTION_STRING")
    blob_conn_str = os.getenv("BLOB_STORAGE_CONN")
    container_name = os.getenv("BLOB_CONTAINER")

    # Blob Service
    blob_service = BlobServiceClient.from_connection_string(blob_conn_str)

    now = datetime.datetime.utcnow()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    timestamp = int(now.timestamp())

    # File path
    blob_path = f"orders/{year}/{month}/{day}/orders-{timestamp}.ndjson"
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_path)

    # Connect to SQL
    conn = pyodbc.connect(sql_conn_str)
    cursor = conn.cursor()

    # Select orders older than 30 days
    cursor.execute("""
        SELECT COUNT(*) FROM Orders 
        WHERE CreatedAt < DATEADD(day, -30, GETDATE())
    """)
    total_records = cursor.fetchone()[0]

    if total_records == 0:
        logging.info("No records older than 30 days. Exiting.")
        return

    logging.info(f"Total records to archive: {total_records}")

    # Start writing to blob
    blob_stream = blob_client.upload_blob(b"", overwrite=True)

    offset = 0
    archived_ids = []

    while True:
        cursor.execute(f"""
            SELECT TOP {BATCH_SIZE} *
            FROM Orders
            WHERE CreatedAt < DATEADD(day, -30, GETDATE())
            ORDER BY OrderId
        """)

        rows = cursor.fetchall()
        if not rows:
            break

        ndjson_lines = []
        for row in rows:
            record = {
                "OrderId": row.OrderId,
                "CustomerName": row.CustomerName,
                "Amount": float(row.Amount),
                "Status": row.Status,
                "CreatedAt": str(row.CreatedAt)
            }
            ndjson_lines.append(json.dumps(record))
            archived_ids.append(row.OrderId)

        # Append batch to blob file
        blob_client.append_block("\n".join(ndjson_lines).encode("utf-8"))

        logging.info(f"Archived batch of {len(rows)} rows")

    # Delete archived rows
    if archived_ids:
        id_batches = [archived_ids[i:i+500] for i in range(0, len(archived_ids), 500)]
        for batch in id_batches:
            ids = ",".join(map(str, batch))
            cursor.execute(f"DELETE FROM Orders WHERE OrderId IN ({ids})")
            conn.commit()

        logging.info(f"Deleted {len(archived_ids)} archived rows from SQL")

    logging.info(f"Archive blob created: {blob_path}")
    logging.info("ArchiveOrders Function Completed")
