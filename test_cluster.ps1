Write-Host "Cleaning up old instances..."
Stop-Process -Name "ddb" -Force -ErrorAction SilentlyContinue
Stop-Process -Name "main" -Force -ErrorAction SilentlyContinue

Write-Host "Building project..."
go build -o ddb.exe

Write-Host "Starting Node 1 (Port 8081)..."
Start-Process ".\ddb.exe" -ArgumentList "-id=1 -port=8081 -peers=http://localhost:8082,http://localhost:8083" -NoNewWindow

Start-Sleep -Seconds 1

Write-Host "Starting Node 2 (Port 8082)..."
Start-Process ".\ddb.exe" -ArgumentList "-id=2 -port=8082 -peers=http://localhost:8081,http://localhost:8083" -NoNewWindow

Start-Sleep -Seconds 1

Write-Host "Starting Node 3 (Port 8083)..."
Start-Process ".\ddb.exe" -ArgumentList "-id=3 -port=8083 -peers=http://localhost:8081,http://localhost:8082" -NoNewWindow

Start-Sleep -Seconds 5

Write-Host "Testing cluster..."
Write-Host "Creating DB 'mydb' on Node 1..."
Invoke-RestMethod -Uri http://localhost:8081/db/create -Method Post -Body '{"db": "mydb"}' -ContentType "application/json"

Start-Sleep -Seconds 1
Write-Host "Creating Table 'users' on Node 1..."
Invoke-RestMethod -Uri http://localhost:8081/table/create -Method Post -Body '{"db": "mydb", "table": "users", "attributes": ["id", "name"]}' -ContentType "application/json"

Start-Sleep -Seconds 1
Write-Host "Inserting record on Node 2 (Follower, should forward to Leader)..."
Invoke-RestMethod -Uri http://localhost:8082/query/insert -Method Post -Body '{"db": "mydb", "table": "users", "record": {"id": 1, "name": "Alice"}}' -ContentType "application/json"

Start-Sleep -Seconds 1
Write-Host "Reading record from Node 3 (Follower)..."
$response = Invoke-RestMethod -Uri http://localhost:8083/query/select -Method Post -Body '{"db": "mydb", "table": "users", "query": {}}' -ContentType "application/json"
$response | ConvertTo-Json -Depth 5

Write-Host "Test complete! Close the node windows to stop them."
