
docker build -t psql -f ./Dockerfile.psql .

docker build -t historico -f ./Dockerfile.historico .

docker build -t loja -f ./Dockerfile.loja .

docker build -t dash -f ./Dockerfile.dash .

docker build -t txt -f ./Dockerfile.txt_behaviour .

docker build -t queue -f ./Dockerfile.queue .

docker build -t webhook -f ./Dockerfile.webhook .

docker build -t web_gen -f ./Dockerfile.web_gen .

docker build -t broker -f ./Dockerfile.broker .

