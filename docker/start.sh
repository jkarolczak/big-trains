docker container start -f $(docker container ls -aq -f name=^/cas[0-9]*$)
