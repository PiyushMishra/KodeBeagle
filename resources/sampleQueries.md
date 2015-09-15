#Query to fetch repositories

```
curl 'http://localhost:9201/repository/_search?size=750' --data '{"query":{"match_all":{}},"sort":[{"stargazersCount":{"order":"desc"}}]}'
```

#Search by import

```
curl 'http://localhost:9201/kodebeagle/_search?size=50' --data '{"query":{"bool":{"must":[{"term":{"custom.tokens.importName":"java.nio.MappedByteBuffer"}}],"must_not":[],"should":[]}},"sort":[{"score":{"order":"desc"}}]}'
```

#Search by import - with wildcard

```
curl 'http://localhost:9201/kodebeagle/_search?size=50' --data '{"query":{"bool":{"must":[{"wildcard":{"custom.tokens.importName":"*FileChannel"}},{"wildcard":{"custom.tokens.importName":"*MappedByteBuffer"}}],"must_not":[],"should":[]}},"sort":[{"score":{"order":"desc"}}]}'
```

#Query to fetch file

```
curl 'http://localhost:9201/sourcefile/_search?size=1' --data '{"query":{"term":{"typesourcefile.fileName":"apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/commitlog/CommitLogSegment.java"}}}'
```

#Query to fetch frequently used methods

```
curl 'http://localhost:9201/fpgrowth/patterns/_search?size=10' --data '{"query":{"filtered":{"query":{"bool":{"must":[{"term":{"body":"java.nio.channels.FileChannel"}}]}},"filter":{"and":{"filters":[{"term":{"length":"1"}}],"_cache":true}}}},"sort":[{"freq":{"order":"desc"}},"_score"]}'
```

#Query to fetch documentation

```
curl 'http://192.168.2.28/api/java/nio/channels/FileChannel.html'
```

#Search by import

```
curl 'http://localhost:9201/importsmethods/_search?size=50' --data '{"query":{"bool":{"must":[{"term":{"typeimportsmethods.tokens.importName":"java.nio.MappedByteBuffer"}}],"must_not":[],"should":[]}},"sort":[{"score":{"order":"desc"}}]}'
```

#Search by import - with wildcard

```
curl 'http://localhost:9201/importsmethods/_search?size=50' --data '{"query":{"bool":{"must":[{"wildcard":{"typeimportsmethods.tokens.importName":"*FileChannel"}},{"wildcard":{"typeimportsmethods.tokens.importName":"*MappedByteBuffer"}}],"must_not":[],"should":[]}},"sort":[{"score":{"order":"desc"}}]}'
```

#Query to search on new index for methods

```
curl -XGET http://localhost:9201/importsmethods/_search?pretty=true -d '{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "typeimportsmethods.tokens.importName": "java.io.File"
          }
        },
        {
          "term": {
            "typeimportsmethods.tokens.methodAndLineNumbers.methodName": "exists"
          }
        },
        {
          "term": {
            "typeimportsmethods.tokens.importName": "java.util.Set"
          }
        }
      ],
      "must_not": [],
      "should": []
    }
  },
  "from": 0,
  "size": 10,
  "sort":[{"score":{"order":"desc"}}],
  "facets": {}
}'
```

#wildcard Query to search on new index for methods

```
curl -XGET http://localhost:9201/importsmethods/_search?pretty=true -d '{
  "query": {
    "bool": {
      "must": [
        {
          "wildcard": {
            "typeimportsmethods.tokens.importName": "*File"
          }
        },
        {
          "term": {
            "typeimportsmethods.tokens.methodAndLineNumbers.methodName": "isDirectory"
          }
        },
        {
          "term": {
            "typeimportsmethods.tokens.importName": "java.util.Set"
          }
        }
      ],
      "must_not": [],
      "should": []
    }
  },
  "from": 0,
  "size": 10,
  "sort":[{"score":{"order":"desc"}}],
  "facets": {}
}'
```

#Query in the nested format in order to specify imports and related methods.

```
curl -XGET http://localhost:9201/importsmethods/_search?pretty=true -d '{  
   "query":{  
      "bool":{  
         "must":[  
                {"nested":{  
                  "path":"tokens",
                  "query" : {  
                     "bool":{  
                        "must":[  
                           {  
                              "term":{  
                                 "tokens.importName":"java.io.File"
                              }
                           },
                           {  
                              "terms":{  
                                 "tokens.methodAndLineNumbers.methodName":[  
                                    "isDirectory"
                                 ],
                                 "minimum_should_match":1
                              }
                           }
                        ]
                     }
                  }
               }
            },
           {"nested":{  
                  "path":"tokens",
                  "query" : {  
                     "bool":{  
                        "must":[  
                           {  
                              "term":{  
                                 "tokens.importName":"java.util.Set"
                              }
                           },
                           {  
                              "terms":{  
                                 "tokens.methodAndLineNumbers.methodName":[  
                                    "add",
                                    "addAll"
                                 ],
                                 "minimum_should_match":1
                              }
                           }
                        ]
                     }
                  }
               }
            }
         ]
      }
   }
}'

```
#wildcard Query in the nested format in order to specify imports and related methods.

```
curl -XGET http://localhost:9201/importsmethods/_search?pretty=true -d '{  
   "query":{  
      "bool":{  
         "must":[  
                {"nested":{  
                  "path":"tokens",
                  "query" : {  
                     "bool":{  
                        "must":[  
                           {  
                              "wildcard":{  
                                 "tokens.importName":"*File"
                              }
                           },
                           {  
                              "terms":{  
                                 "tokens.methodAndLineNumbers.methodName":[  
                                    "isDirectory"
                                 ],
                                 "minimum_should_match":1
                              }
                           }
                        ]
                     }
                  }
               }
            },
           {"nested":{  
                  "path":"tokens",
                  "query" : {  
                     "bool":{  
                        "must":[  
                           {  
                              "term":{  
                                 "tokens.importName":"java.util.Set"
                              }
                           },
                           {  
                              "terms":{  
                                 "tokens.methodAndLineNumbers.methodName":[  
                                    "add",
                                    "addAll"
                                 ],
                                 "minimum_should_match":1
                              }
                           }
                        ]
                     }
                  }
               }
            }
         ]
      }
   }
}'
```
# Query using nested filters for imports and related methods

```
curl -XGET http://localhost:9201/importsmethods/_search?pretty=true -d '{
  "query": {
    "filtered": {
      "filter": {
        "and": [
          {
            "nested": {
              "path": "tokens",
              "filter": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "tokens.importName": "java.util.linkedlist"
                      }
                    }
                  ],
                  "should": [
                    {
                      "terms": {
                        "tokens.methodAndLineNumbers.methodName": [
                          "add"
                        ]
                      }
                    }
                  ]
                }
              }
            }
          },
          {
            "nested": {
              "path": "tokens",
              "filter": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "tokens.importName": "javax.swing.jbutton"
                      }
                    }
                  ],
                  "should": [
                    {
                      "terms": {
                        "tokens.methodAndLineNumbers.methodName": [
                          "setText"
                        ]
                      }
                    }
                  ]
                }
              }
            }
          }
        ]
      },
      "_cache":true
    }
  }
}'
```

#Query using filter for imports

```
curl -XGET http://localhost:9201/importsmethods/_search?pretty=true -d'{
  "query": {
    "filtered": {
      "filter": {
        "and": [
          {
            "term": {
              "tokens.importName": "java.util.list"
            }
          },
          {
            "term": {
              "tokens.importName": "java.util.hashmap"
            }
          }
        ]
      },
      "_cache":true
    }
  }
}'

```
