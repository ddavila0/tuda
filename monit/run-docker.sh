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

echo "Building monicron image..."
docker build --tag=monicron .
echo "Collecting volume..."
volume=`docker volume ls --quiet`
if [[ -z ${volume} ]]; then
    volume=`docker volume create monicron`
fi
echo "Starting container..."
container=`docker run -d -it -p 5001-5300:5001-5300 -v monicron:/aggregations --hostname tuda.cern.ch monicron:latest`
echo "Intialized container named ${container}"
input_loop ${container}
