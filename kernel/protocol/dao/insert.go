package dao

//curl -X POST \
// -H "Content-Type: application/vnd.ksql.v1+json" \
// -d '{
//   "ksql": "INSERT INTO TESTIFY (ID) VALUES (1);"
// }' \
// http://localhost:8088/ksql

type (
	InsertResponse []struct{}
)

//[]%
