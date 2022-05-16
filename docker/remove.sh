docker stop  $(docker container ls -aq -f name=^/cas[0-9]*$)
docker rm -f $(docker container ls -aq -f name=^/cas[0-9]*$)
