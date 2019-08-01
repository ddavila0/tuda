echo "Building monicron image..."
docker build --tag=monicron .
echo "Starting container..."
container=`docker run -d -it -p 5001-5300:5001-5300 --hostname tuda.cern.ch monicron:latest`
echo "Intialized container named ${container}"
