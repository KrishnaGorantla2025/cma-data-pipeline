# cma-data-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# run
python src/ingest.py \
  --listings data/listings.csv \
  --lookup  data/category_lookup.csv \
  --outdir  output/
