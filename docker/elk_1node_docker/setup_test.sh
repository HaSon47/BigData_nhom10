# curl -X POST "http://localhost:9200/_security/user/test" \
#      -H "Content-Type: application/json" \
#      -u elastic:123123 \
#      --insecure \
#      -d '{
#        "password" : "123123",
#        "roles" : [ "user" ],
#        "full_name" : "Test User",
#        "email" : "test@example.com"
#      }'
# curl -X GET "http://localhost:9200/_cluster/health?pretty" -u elastic:123123
# curl -X GET "http://localhost:9200/_cat/nodes?v" -u elastic:123123
# curl -X GET "http://localhost:9200/_cat/shards?v" -u elastic:123123
# curl 'localhost:9200/_cat/indices?v' -u elastic:123123

curl -X GET "localhost:9200/test_insert_data1/_search?pretty" -u elastic:123123 -H 'Content-Type: application/json' -d'
{
  "size": 100,
  "query": {
    "match_all": {}
  }
}
'



# curl -X POST "http://localhost:9200/_cluster/reroute" -H "Content-Type: application/json" -u elastic:123123 -d '{
#   "commands": [
#     {
#       "allocate_empty_primary": {
#         "index": "your_index",
#         "shard": 0,
#         "node": "elasticsearch",
#         "accept_data_loss": true
#       }
#     }
#   ]
# }'
