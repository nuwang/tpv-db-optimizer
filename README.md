# usegalaxy-federation-to-tpv-db
Generate updates for the TPV shared db based on usegalaxy.* federation stats

# installation

```bash
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
```

# usage

1. Make sure that the views defined in views.py are already deployed in your database
2. Run `python mem-optimize.py "postgresql+asyncpg://username:password@localhost/galaxy"` which will generate output.yaml.
   This file will contain wastage stats for each tool.
3. Run `python update-shared-db.py /path/to/tpv-shared-database/tools.yml output.yaml`.
   This will update resource requirements in tpv's shared database based on the wastage stats in output.yaml.
