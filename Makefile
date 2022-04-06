


data:
	mkdir -p data

data/statements.latest.jsonl.gz: data
	curl -o data/statements.latest.jsonl.gz https://oo-register-production.s3-eu-west-1.amazonaws.com/public/exports/statements.latest.jsonl.gz

data/bods-entities.ftm.ijson:
	ftm store iterate -d bods-registry -o data/bods-entities.ftm.ijson

clean:
	rm -rf data/*