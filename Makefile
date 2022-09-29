


data:
	mkdir -p data

data/statements.latest.jsonl.gz: data
	curl -s -o data/statements.latest.jsonl.gz https://oo-register-production.s3-eu-west-1.amazonaws.com/public/exports/statements.latest.jsonl.gz

data/fragments.json: data/statements.latest.jsonl.gz
	python parse.py

data/sorted.json: data/fragments.json
	sort -o data/sorted.json data/fragments.json

data/openownership.json: data/sorted.json
	ftm sorted-aggregate -i data/sorted.json -o data/openownership.json

clean:
	rm -rf data/*