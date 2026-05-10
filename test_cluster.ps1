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
Start-Sleep -Seconds 1

Write-Host "All 3 nodes started successfully!"
Write-Host "Opening GUI for Node 1, 2, and 3..."

# Open browser to the GUI
Start-Process "http://localhost:8081/"
Start-Process "http://localhost:8082/"
Start-Process "http://localhost:8083/"

Write-Host "Test complete! Close the terminal windows to stop the nodes."
