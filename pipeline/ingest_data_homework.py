import click
import pandas as pd
from sqlalchemy import create_engine

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='pgdatabase', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--parquet-file', default='green_tripdata_2025-11.parquet', type=click.Path(exists=True), help='Path to parquet file')
@click.option('--zone-file', default='taxi_zone_lookup.csv', type=click.Path(exists=True), help='Path to zone lookup CSV')
@click.option('--chunksize', default=100_000, type=int, help='Chunk size for ingestion')
@click.option('--trip-table', default='green_taxi_data', help='Target table name for trips')
@click.option('--zone-table', default='taxi_zones', help='Target table name for zones')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, parquet_file, zone_file, chunksize, trip_table, zone_table):

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # Ingest parquet file
    print(f"Ingesting {parquet_file}...")
    df = pd.read_parquet(parquet_file)
    print(f"Read {len(df)} rows from parquet file")
    
    # Ingest in chunks
    for i in range(0, len(df), chunksize):
        chunk = df.iloc[i:i+chunksize]
        chunk.to_sql(
            name=trip_table,
            con=engine,
            if_exists="replace" if i == 0 else "append",
            index=False,
        )
        print(f"Inserted chunk, rows: {len(chunk)}")
    
    # Ingest zone lookup
    print(f"\nIngesting {zone_file}...")
    df_zones = pd.read_csv(zone_file)
    print(f"Read {len(df_zones)} zones")
    
    df_zones.to_sql(
        name=zone_table,
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"Inserted {len(df_zones)} zones into {zone_table}")

if __name__ == "__main__":
    run()
