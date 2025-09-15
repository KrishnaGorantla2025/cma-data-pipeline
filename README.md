# cma-data-pipeline

A compact, production-minded ETL that **ingests -> validates/cleans -> de-duplicates -> enriches -> writes Parquet** and emits a **JSON data-quality report**.  
Designed for AWS productionisation (slides in `docs/`).

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# run
python src/ingest.py \
  --listings data/listings.csv \
  --lookup  data/category_lookup.csv \
  --outdir  output/
