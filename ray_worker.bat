@echo off
setlocal enabledelayedexpansion

REM Check if Docker is installed
where docker >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Docker is not installed. Please install Docker and try again.
    exit /b 1
)

REM Check if the required arguments are provided
if "%~4"=="" (
    echo Usage: %0 ^<head_node_ip^> ^<provider_id^> ^<username^> ^<password^>
    exit /b 1
)

set HEAD_NODE_IP=%~1
set PROVIDER_ID=%~2
set USERNAME=%~3
set PASSWORD=%~4

REM Get the auth token
curl -s -X POST "http://67.205.167.215:8000/token" ^
     -H "Content-Type: application/x-www-form-urlencoded" ^
     -d "username=%USERNAME%&password=%PASSWORD%" > token_response.txt

set /p TOKEN_RESPONSE=<token_response.txt
del token_response.txt

REM Extract access token (simplified version)
for /f "tokens=2 delims=:," %%a in ('echo %TOKEN_RESPONSE% ^| findstr "access_token"') do (
    set AUTH_TOKEN=%%a
    set AUTH_TOKEN=!AUTH_TOKEN:"=!
)

if "!AUTH_TOKEN!"=="" (
    echo Failed to get auth token. Please check your credentials.
    echo Server response: %TOKEN_RESPONSE%
    exit /b 1
)

echo Successfully obtained auth token.

REM Pull the custom Ray image
docker pull piyushaaryan/ioc:1.1

REM Run the Ray container
docker run --rm ^
    --name ray-worker-%PROVIDER_ID% ^
    --network host ^
    --shm-size=1gb ^
    -e RAY_ADDRESS="ray://%HEAD_NODE_IP%:10001" ^
    -e PROVIDER_ID="%PROVIDER_ID%" ^
    -e AUTH_TOKEN="%AUTH_TOKEN%" ^
    piyushaaryan/ioc:1.0

endlocal