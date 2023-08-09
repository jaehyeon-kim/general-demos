###### PART 1
# https://github.com/LisaHJung/Beginners-Crash-Course-to-Elastic-Stack-Series-Table-of-Contents

curl localhost:9200

# jq, python -m json.tool
curl localhost:9200/_cluster/health | json_pp
curl localhost:9200/_nodes/stats | json_pp

#### CRUD
curl -X PUT localhost:9200/favourite_candy
curl localhost:9200/ | json_pp

## id will be auto-generated
curl -X POST localhost:9200/favourite_candy/_doc \
  -H 'Content-Type: application/json' \
  -d '{"first_name":"Lisa","candy":"Sour Skittles"}'
curl localhost:9200/favourite_candy/_doc/qoix2IkBueezf-_gAixr | json_pp

## upsert a document, version will be updated for update
curl -X PUT localhost:9200/favourite_candy/_doc/0 \
  -H 'Content-Type: application/json' \
  -d '{"first_name":"John","candy":"Starburst"}'
curl localhost:9200/favourite_candy/_doc/0 | json_pp

## fail if id exists
curl -X PUT localhost:9200/favourite_candy/_create/0 \
  -H 'Content-Type: application/json' \
  -d '{"first_name":"John","candy":"Starburst"}'

## update
curl -X POST localhost:9200/favourite_candy/_update/0 \
  -H 'Content-Type: application/json' \
  -d '{"doc":{"candy": "Lolli Pop"}}'

## delete
curl -X DELETE localhost:9200/favourite_candy/_doc/0