@echo off
echo Building and Starting Docker Containers...

docker build -t bookimpact -f Dockerfile .
docker run bookimpact

if %errorlevel% neq 0 (
    echo Error occurred during the build or start process. Exiting...
    exit /b %errorlevel%
)
