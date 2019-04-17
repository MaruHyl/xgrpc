# xgrpc
grpc connection manager, especially for kit factory

Ref: https://github.com/go-kit/kit/blob/master/examples/apigateway/main.go#L184

## Example

```go

var m =xgrpc.NewManager()

func gRpcFactory(instance string) (endpoint.Endpoint, io.Closer, error) {

	// get Conn
	conn, err := m.GetConn(context.Background(),instance)
	if err != nil {
		return nil, nil, err
	}
	
	// get gRpcConn & closer
	gRpcConn, closer, err:=conn.Get(context.Background())
	if err != nil {
		return nil, nil, err
	}

	// ...
	// make endpoint

	return endpoint, closer, nil
}
```
