# project1-database-proxy-golang

```bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    database/databaseproxy.proto
```

```go
if !collection.Contains(sql.Drivers(), "database-proxy") {
    sql.Register("database-proxy", database.NewDriver(&database.DriverConfig{
        NewGrpcConn: func(node string) *grpc.ClientConn {
            log.Printf("connecting to database proxy at %s\n", node)
            sb := strings.Builder{}
            sb.WriteString(node)
            sb.WriteString(":")
            sb.WriteString(this.ConfigurationDto.App.DatabaseProxy.Port)
            if grpcConn, err := grpc.Dial(sb.String(), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
                log.Printf("fail to dial: %v", err)
                return nil
            } else {
                return grpcConn
            }
        }}))
}
```
