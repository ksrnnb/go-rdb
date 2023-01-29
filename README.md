# go-rdb

# How to use
Start Database with the following command. Data is stored in simpledb directory.

```bash
go run main.go
```

Then in other terminal, you can execute SQL using curl command.

## Create table
```bash
$ curl -s localhost:8888 -d "{\"query\": \"CREATE TABLE users (uid int, name varchar(16))\"}" | jq
# {
#   "message": "0 records has changed"
# }
```

```bash
$ curl -s localhost:8888 -d "{\"query\": \"CREATE TABLE profiles (pid int, user_id int, address varchar(16))\"}" | jq
# {
#   "message": "0 records has changed"
# }
```

## Insert data
```bash
$ curl -s localhost:8888 -d "{\"query\": \"INSERT INTO users (uid, name) VALUES (1, 'hoge')\"}" | jq
# {
#   "message": "1 records has changed"
# }
```

```bash
$ curl -s localhost:8888 -d "{\"query\": \"INSERT INTO profiles (pid, user_id, address) VALUES (1, 1, 'Tokyo')\"}" | jq
# {
#   "message": "1 records has changed"
# }
```

## Update data
```bash
$ curl -s localhost:8888 -d "{\"query\": \"UPDATE users SET name='piyopiyo' WHERE uid=1\"}" | jq
# {
#   "message": "1 records has changed"
# }
```

## Select data
```bash
$ curl -s localhost:8888 -d "{\"query\": \"SELECT uid, name FROM users\"}" | jq
# [
#   {
#     "uid": 1,
#     "name": "piyopiyo"
#   },
# ]
```

## Join data
```bash
$ curl -s localhost:8888 -d "{\"query\": \"SELECT uid, name, pid, user_id, address FROM users, profiles WHERE uid=user_id\"}" | jq
# [
#   {
#     "address": "Tokyo",
#     "name": "piyopiyo",
#     "pid": 1,
#     "uid": 1,
#     "user_id": 1
#   }
# ]
```

## Delete records
```bash
$ curl -s localhost:8888 -d "{\"query\": \"DELETE FROM users WHERE uid=1\"}" | jq
# {
#   "message": "1 records has changed"
# }
```

# References
- [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)
