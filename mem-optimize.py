import argparse
import asyncio
from decimal import Decimal
import logging
import sys
import yaml

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import text
import pandas as pd

# Configure logging to print to stdout
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

# Create a stream handler to print to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
log.addHandler(handler)


# SQL Query
SQL_MEM_QUERY_COMMON = '''
SELECT tpv_tool_name, count(tpv_tool_name) as num_jobs, AVG(runtime) as runtime,
  AVG(tpv_mem_gb) as avg_tpv_mem_gb,
  MAX(tpv_mem_gb) as max_tpv_mem_gb,
  AVG(tpv_cores) as avg_tpv_cores,
  MAX(tpv_cores) as max_tpv_cores,
  count(tool_id) as job_count,
  AVG(job_max_mem_gb) as avg_job_max_mem_gb,
  MAX(job_max_mem_gb) as max_job_max_mem_gb,
  AVG(GREATEST(tpv_mem_gb - job_max_mem_gb, 0)) as mem_wastage_avg_gb,
  MIN(GREATEST(tpv_mem_gb - job_max_mem_gb, 0)) as mem_wastage_min_gb
  COALESCE(AVG(GREATEST(tpv_mem_gb - job_max_mem_gb, 0) / NULLIF(tpv_mem_gb, 0) * 100), 0) as mem_wastage_avg_percentage,
  COALESCE(MIN(GREATEST(tpv_mem_gb - job_max_mem_gb, 0) / NULLIF(tpv_mem_gb, 0) * 100), 0) as mem_wastage_min_percentage
'''

SQL_MEM_QUERY_AU = f'''
{SQL_MEM_QUERY_COMMON}
FROM au_job_resource_usage_view WHERE
  updated >= CURRENT_DATE - INTERVAL '1 year' GROUP BY tpv_tool_name, tool_name ORDER BY count(tpv_tool_name);
'''

SQL_MEM_QUERY_EU = f'''
{SQL_MEM_QUERY_COMMON}
FROM eu_job_resource_usage_view WHERE
  updated >= CURRENT_DATE - INTERVAL '1 year' GROUP BY tpv_tool_name, tool_name ORDER BY count(tpv_tool_name);
'''

SQL_MEM_QUERY_US = f'''
{SQL_MEM_QUERY_COMMON}
FROM us_job_resource_usage_view WHERE
  -- Stampede2 does not isolate jobs in cgroups so we filter it out
  destination NOT LIKE 'stampede%' AND 
  updated >= CURRENT_DATE - INTERVAL '1 year' GROUP BY tpv_tool_name, tool_name ORDER BY count(tpv_tool_name);
'''

# List of queries
SQL_QUERIES = [
    SQL_MEM_QUERY_AU,
    SQL_MEM_QUERY_EU,
    SQL_MEM_QUERY_US
]


async def fetch_data(engine, query):
    async with engine.connect() as connection:
        result = await connection.execute(text(query))
        rows = result.fetchall()
        log.debug(f'Query executed, fetched {len(rows)} rows')
        return pd.DataFrame(rows, columns=result.keys())

async def fetch_all_data(database_uris):
    engines = [create_async_engine(uri, echo=True) for uri in database_uris]
    tasks = []
    
    for engine in engines:
        for query in SQL_QUERIES:
            tasks.append(fetch_data(engine, query))
    
    results = await asyncio.gather(*tasks)

    # Clean up all engines
    for engine in engines:
        await engine.dispose()

    return results

async def main(database_uris):
    log.debug(f'Starting data fetch for databases: {database_uris}')
    results = await fetch_all_data(database_uris)
        
    # Combine all DataFrames into one
    combined_df = pd.concat(results)

    # Find minimum mem_wastage_min per tool across all results
    min_mem_wastage = combined_df.groupby('tpv_tool_name', as_index=False).agg({
        'mem_wastage_min_gb': 'min',
        'avg_tpv_mem_gb': 'mean',
        'max_tpv_mem_gb': 'max',
        'max_job_max_mem_gb': 'max',
        'num_jobs': 'sum'
    })

    # Convert all relevant columns to floats
    min_mem_wastage['mem_wastage_min_gb'] = min_mem_wastage['mem_wastage_min_gb'].astype(float)
    min_mem_wastage['avg_tpv_mem_gb'] = min_mem_wastage['avg_tpv_mem_gb'].astype(float)
    min_mem_wastage['max_tpv_mem_gb'] = min_mem_wastage['avg_tpv_mem_gb'].astype(float)
    min_mem_wastage['max_job_max_mem_gb'] = min_mem_wastage['max_job_max_mem_gb'].astype(float)
    min_mem_wastage['num_jobs'] = min_mem_wastage['num_jobs'].astype(float)
    log.debug('Data processing complete')

    # Calculate weighted mem_wastage_min across all rows
    total_jobs = combined_df['num_jobs'].sum()
    weighted_mem_wastage_min = (combined_df['mem_wastage_min'] * combined_df['num_jobs']).sum() / total_jobs

    # Calculate weighted minimum wastage as a percentage using mem_wastage_min_percentage column across all rows
    weighted_mem_wastage_percentage = (combined_df['mem_wastage_min_percentage'] * combined_df['num_jobs']).sum() / total_jobs

    # Log the weighted mem_wastage_min and percentage
    log.debug(f'Weighted mem_wastage_min: {weighted_mem_wastage_min}')
    log.debug(f'Weighted mem_wastage_min percentage: {weighted_mem_wastage_percentage}%')

    # Add a 5% overhead to mem_wastage_min in GB
    min_mem_wastage['mem_wastage_min_gb'] = min_mem_wastage['mem_wastage_min_gb'] * 0.95

    # Filter DataFrame to include only rows where mem_wastage_min > 0.1 GB
    filtered_df = min_mem_wastage[min_mem_wastage['mem_wastage_min_gb'] > 0.1]
    
    # Convert filtered DataFrame to dictionary
    filtered_data_dict = filtered_df.set_index('tpv_tool_name').T.to_dict()

    # Write dictionary to YAML file
    with open('output.yaml', 'w') as yaml_file:
        yaml.dump(filtered_data_dict, yaml_file, default_flow_style=False)
    
    log.debug('Data written to output.yaml')


DEFAULT_DATABASE_URIS = [
    f'postgresql+asyncpg://username:password@localhost/galaxy'
]

def parse_arguments():
    parser = argparse.ArgumentParser(description='Run SQL queries on multiple databases asynchronously.')
    parser.add_argument('database_uris', metavar='N', type=str, nargs='*', help=f'Database URIs to connect to, for example: {DEFAULT_DATABASE_URIS}')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    asyncio.run(main(args.database_uris))
