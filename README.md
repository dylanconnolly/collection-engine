1. Set configurable attributes in `config.yaml`
1. Build image `docker build -t collection-engine .`
2. Run container `docker run collection-engine`

## Testing
1. Run all tests `go run -v -race ./engine/...`
2. Run specific test file `go run -v -race ./engine/<filename>_test.go`
3. Report coverage `go test -coverprofile cover.out ./engine/...`
4. View coverage report in browser `go tool cover -html=cover.out`