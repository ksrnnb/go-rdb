# go-rdb

# How to use
Start Database with the following command.

```bash
go run main.go
```

Then in other terminal, you can execute SQL using curl command.

## Create table
```bash
$ curl -s localhost:8888 -d "{\"query\": \"CREATE TABLE users (id int, name varchar(16))\"}" | jq
# {
#   "message": "0 records has changed"
# }
```

## Insert data
```bash
$ curl -s localhost:8888 -d "{\"query\": \"INSERT INTO users (id, name) VALUES (1, 'hoge')\"}" | jq
# {
#   "message": "1 records has changed"
# }
```

## Update field
```bash
$ curl -s localhost:8888 -d "{\"query\": \"UPDATE users SET name='piyopiyo' WHERE id=1\"}" | jq
# {
#   "message": "1 records has changed"
# }
```

## Select data
```bash
$ curl -s localhost:8888 -d "{\"query\": \"SELECT id, name FROM users\"}" | jq
# [
#   {
#     "id": 1,
#     "name": "piyopiyo"
#   },
# ]
```

## Delete records
```bash
$ curl -s localhost:8888 -d "{\"query\": \"DELETE FROM users WHERE id=1\"}" | jq
# {
#   "message": "1 records has changed"
# }
```

# References
- [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)
