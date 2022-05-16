docker stop $(docker container ls -aq -f name=^/cas[0-9]*$)
