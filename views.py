JOB_RESOURCE_VIEW_AU = '''
CREATE MATERIALIZED VIEW public.au_job_resource_usage_view AS
SELECT job_id, updated, tool_id, COALESCE(tool_name, tool_id) as tool_name, COALESCE(tpv_tool_name, tool_id) as tpv_tool_name, tpv_cores, tpv_mem_gb, job_max_mem_gb, runtime, actual_cpu_usage, tpv_cores*runtime as allocated_cpu_hours, destination FROM (
        SELECT
                j.id as job_id,
                j.update_time as updated,
                j.tool_id as tool_id,
                (REGEXP_MATCHES(j.tool_id, 'toolshed.g2.bx.psu.edu/repos/.*/.*/(.*)/.*'))[1] as tool_name,
                (REGEXP_MATCHES(j.tool_id, '(toolshed.g2.bx.psu.edu/repos/.*/.*/.*)/.*'))[1] as tpv_tool_name,
                CAST((REGEXP_MATCHES(encode(j.destination_params, 'escape'), 'ntasks=(\d+)'))[1] AS NUMERIC) as tpv_cores,
                CAST((REGEXP_MATCHES(encode(j.destination_params, 'escape'), 'mem=(\d+)'))[1] AS NUMERIC)/1024.0 as tpv_mem_gb,
                (SELECT 
                        jmn.metric_value/1024.0/1024.0/1024.0
                        FROM public.au_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'memory.max_usage_in_bytes'
                        AND jmn.job_id = j.id LIMIT 1
                ) as job_max_mem_gb,
                (SELECT 
                        jmn.metric_value
                        FROM public.au_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'runtime_seconds'
                        AND jmn.job_id = j.id LIMIT 1
                ) as runtime,
                (SELECT 
                        jmn.metric_value/1e+9
                        FROM public.au_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'cpuacct.usage'
                        AND jmn.job_id = j.id LIMIT 1
                ) as actual_cpu_usage,
                j.destination_id as destination
        FROM public.au_job j
        WHERE j.state in ('ok')
) WHERE job_max_mem_gb IS NOT NULL;
'''

JOB_RESOURCE_INDEX_AU = '''
CREATE INDEX idx_tool_id_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (tool_id);
CREATE INDEX idx_tool_name_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (tool_name);
CREATE INDEX idx_tpv_tool_name_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (tpv_tool_name);
CREATE INDEX idx_destination_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (destination);
CREATE INDEX idx_job_max_mem_gb_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (job_max_mem_gb);
CREATE INDEX idx_runtime_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (runtime);
CREATE INDEX idx_actual_cpu_usage_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (actual_cpu_usage);
CREATE INDEX idx_allocated_cpu_hours_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (allocated_cpu_hours);
CREATE INDEX idx_updated_on_au_job_resource_usage_view ON public.au_job_resource_usage_view (updated);
'''

JOB_RESOURCE_VIEW_EU = '''
CREATE MATERIALIZED VIEW public.eu_job_resource_usage_view AS
SELECT job_id, updated, tool_id, COALESCE(tool_name, tool_id) as tool_name, COALESCE(tpv_tool_name, tool_id) as tpv_tool_name, tpv_cores, tpv_mem_gb, job_max_mem_gb, runtime, actual_cpu_usage, tpv_cores*runtime as allocated_cpu_hours, destination FROM (
        SELECT
                j.id as job_id,
                j.update_time as updated,
                j.tool_id as tool_id,
                (REGEXP_MATCHES(j.tool_id, 'toolshed.g2.bx.psu.edu/repos/.*/.*/(.*)/.*'))[1] as tool_name,
                (REGEXP_MATCHES(j.tool_id, '(toolshed.g2.bx.psu.edu/repos/.*/.*/.*)/.*'))[1] as tpv_tool_name,
                public.safe_convert_to_numeric(replace(encode(j.destination_params, 'escape'), E'\\\\', E'\\')::json, 'request_cpus') as tpv_cores,
                public.convert_to_mb(replace(encode(j.destination_params, 'escape'), E'\\\\', E'\\')::json->>'request_memory')/1024.0 as tpv_mem_gb,
                (SELECT 
                        jmn.metric_value/1024.0/1024.0/1024.0
                        FROM public.eu_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'memory.max_usage_in_bytes'
                        AND jmn.job_id = j.id LIMIT 1
                ) as job_max_mem_gb,
                (SELECT 
                        jmn.metric_value
                        FROM public.eu_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'runtime_seconds'
                        AND jmn.job_id = j.id LIMIT 1
                ) as runtime,
                (SELECT 
                        jmn.metric_value/1e+9
                        FROM public.eu_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'cpuacct.usage'
                        AND jmn.job_id = j.id LIMIT 1
                ) as actual_cpu_usage,
                j.destination_id as destination
        FROM public.eu_job j
        WHERE j.state in ('ok')
) WHERE job_max_mem_gb IS NOT NULL;
'''

JOB_RESOURCE_INDEX_EU = '''
CREATE INDEX idx_tool_id_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (tool_id);
CREATE INDEX idx_tool_name_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (tool_name);
CREATE INDEX idx_tpv_tool_name_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (tpv_tool_name);
CREATE INDEX idx_destination_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (destination);
CREATE INDEX idx_job_max_mem_gb_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (job_max_mem_gb);
CREATE INDEX idx_runtime_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (runtime);
CREATE INDEX idx_actual_cpu_usage_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (actual_cpu_usage);
CREATE INDEX idx_allocated_cpu_hours_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (allocated_cpu_hours);
CREATE INDEX idx_updated_on_eu_job_resource_usage_view ON public.eu_job_resource_usage_view (updated);
'''

JOB_RESOURCE_VIEW_US =  '''
CREATE MATERIALIZED VIEW public.us_job_resource_usage_view AS
SELECT job_id, updated, tool_id, COALESCE(tool_name, tool_id) as tool_name, COALESCE(tpv_tool_name, tool_id) as tpv_tool_name, tpv_cores, tpv_mem_gb, job_max_mem_gb, runtime, actual_cpu_usage, tpv_cores*runtime as allocated_cpu_hours, destination FROM (
        SELECT
                j.id as job_id,
                j.update_time as updated,
                j.tool_id as tool_id,
                (REGEXP_MATCHES(j.tool_id, 'toolshed.g2.bx.psu.edu/repos/.*/.*/(.*)/.*'))[1] as tool_name,
                (REGEXP_MATCHES(j.tool_id, '(toolshed.g2.bx.psu.edu/repos/.*/.*/.*)/.*'))[1] as tpv_tool_name,
                CAST((REGEXP_MATCHES(encode(j.destination_params, 'escape'), 'ntasks=(\d+)'))[1] AS NUMERIC) as tpv_cores,
                COALESCE(
                    -- Attempt to calculate memory from mem-per-cpu multiplied by the number of cores
                    CAST(SUBSTRING(encode(j.destination_params, 'escape') FROM 'mem-per-cpu=(\d+)') AS NUMERIC) 
                    * CAST(SUBSTRING(encode(j.destination_params, 'escape') FROM 'ntasks=(\d+)') AS NUMERIC),
                    -- Fallback to regular mem if mem-per-cpu is not found
                    CAST(SUBSTRING(encode(j.destination_params, 'escape') FROM 'mem=(\d+)') AS NUMERIC)
                )/1024.0 as tpv_mem_gb,
                (SELECT 
                        jmn.metric_value/1024.0/1024.0/1024.0
                        FROM public.us_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'memory.max_usage_in_bytes'
                        AND jmn.job_id = j.id LIMIT 1
                ) as job_max_mem_gb,
                (SELECT 
                        jmn.metric_value
                        FROM public.us_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'runtime_seconds'
                        AND jmn.job_id = j.id LIMIT 1
                ) as runtime,
                (SELECT 
                        jmn.metric_value/1e+9
                        FROM public.us_job_metric_numeric jmn
                        WHERE jmn.metric_name = 'cpuacct.usage'
                        AND jmn.job_id = j.id LIMIT 1
                ) as actual_cpu_usage,
                j.destination_id as destination
        FROM public.us_job j
        WHERE j.state in ('ok')
) WHERE job_max_mem_gb IS NOT NULL
-- Stampede2 does not isolate jobs in cgroups so we filter it out
AND destination NOT LIKE 'stampede%';
'''

JOB_RESOURCE_INDEX_US = '''
CREATE INDEX idx_tool_id_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (tool_id);
CREATE INDEX idx_tool_name_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (tool_name);
CREATE INDEX idx_tpv_tool_name_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (tpv_tool_name);
CREATE INDEX idx_destination_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (destination);
CREATE INDEX idx_job_max_mem_gb_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (job_max_mem_gb);
CREATE INDEX idx_runtime_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (runtime);
CREATE INDEX idx_actual_cpu_usage_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (actual_cpu_usage);
CREATE INDEX idx_allocated_cpu_hours_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (allocated_cpu_hours);
CREATE INDEX idx_updated_on_us_job_resource_usage_view ON public.us_job_resource_usage_view (updated);
'''
