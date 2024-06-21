# A2-CE
Trabalho final da disciplina Computação Escalável na FGV EMAp, lecionada pelo professor Thiago Pinheiro de Araújo.


## Como rodar localmente
Use o comando
```
sudo docker compose up --build
```
para rodar o projeto localmente. Em outro terminal, use o comando
```
docker compose logs -f dash
```
para acessar a URL do streamlit. Copie a URL "Network URL" e cole no navegador para acessar o dashboard.

Note que o dashboard pode apresentar erro de não encontrar dados, isso pode ser porque como são muitos processos para rodar em uma máquina local, pode demorar para tudo chegar no banco de dados final. Se isso acontecer, espere alguns segundos e recarregar a página pode resolver.

O docker compose não está rodando o webhook por padrão, pelo mesmo problema de termos muitos processos rodando ao mesmo tempo. Se você quiser testar com o webhook descomente as linhas 27 a 56 do docker-compose.yaml.

O bonificador/cupom_generator.py não foi configurado para rodar com os docker, por isso não está no docker compose e o mock/lojas.py está com o código relacionado ao bonificado comentado.
