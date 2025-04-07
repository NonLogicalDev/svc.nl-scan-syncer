start-uvicorn:
    #!/bin/bash
    export APP_HOME=$(pwd)/tmp/app
    mkdir -p ${APP_HOME}

    export APP_CFG_DATA_DIR=${APP_HOME}/data 
    export APP_CFG_SRC_DIR=${APP_HOME}/syncer/src 

    export APP_CFG_DST1_DIR=${APP_HOME}/syncer/dst1 
    export APP_CFG_DST1_NICKNAME="(docs/consume)"

    export APP_CFG_DST2_DIR=${APP_HOME}/syncer/dst2 
    export APP_CFG_DST2_NICKNAME="(docs/printer)"

    #uv run uvicorn server:app --host localhost --port 5040 --reload
    uv run python server.py

build-docker:
    docker build -t nl-scan-syncer .

run-docker:
    docker run -p 5040:5040 nl-scan-syncer

