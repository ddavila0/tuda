input_loop() {
    read -p "Attach to container? (y/n) " confirm_attach
    if [[ "${confirm_attach}" == [yY] ]]; then
        docker attach "${1}"
    elif [[ "${confirm_attach}" == [nN] ]]; then
        return
    else
        input_loop ${1}
    fi 
}

existing=`docker ps --quiet`
if [[ -z "${existing}" ]] ; then
    if [[ -e "${1}" ]] ; then
        echo "Building monicron image..."
        docker build --tag=monicron --build-arg keytab="${1}" --build-arg user="${USER}" .
        echo "Collecting volume..."
        volume=`docker volume ls --quiet`
        if [[ -z ${volume} ]]; then
            volume=`docker volume create monicron`
        fi
        echo "Starting container..."
        container=$(docker run -d -it -p 5001-5300:5001-5300 -v monicron:/aggregations --hostname tuda.cern.ch monicron:latest)
        echo "Intialized container named ${container}"
        input_loop ${container}
    else
        echo "Error: No keytab supplied or invalid path"
        echo "Usage: ./run-docker </path/to/kt.keytab>"
        exit 0
    fi
else
    echo "Container already running!"
    input_loop ${existing}
fi

