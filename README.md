# A2-CE
Trabalho final da disciplina Computação Escalável na FGV EMAp, lecionada pelo professor Thiago Pinheiro de Araújo.


## Dashboard

Para rodar o dashboard, é necessário instalar a biblioteca `streamlit`:

TODO: requirement.txt
```
pip install streamlit
pip install plotly
pip install streamlit-aggrid
```

e rodar o seguinte comando:

```
streamlit run dashboard.py
```

obs: 

- O dashboard foi feito para rodar em dark mode, então se você estiver em light mode, o dashboard pode não ficar tão bonito.
 
- Para modificar o tema para dark ou light, clique nos três pontos,depois em settings e escolhe o tema.
 
### Para rodar com WSL:
Instale o `PySpark` no WSL com o comando `pip install pyspark`.

Você ainda tem que baixar o JAVA com `sudo apt update` e depois `sudo apt install openjdk-11-jdk`.

Para rodar o `q1.py` é necessário baixar o postgresql 16 [aqui](https://www.postgresql.org/download/) siga os passos do instalador e não se esqueça de instalar o pgAdmin4. Após a instalação, abra o pgAdmin4 e crie um banco de dados chamado `source_db` com um usuário `postgres` e senha `sua_senha`. O arquivo `example.sql` cria uma tabela `your_table_name` e insere alguns registros nela. O arquivo `q1.py` lê esses registros e filtra os dados e os insere em uma tabela `processed_table_name` no banco de dados `source_db`.

Antes de rodar o `q1.py` no WSL, faça o seguinte:
- Verique dentro do arquivo `postgresql.conf` se o `listen_addresses` está configurado para `*`, geralmente esse que é localizado em `C:\Program Files\PostgreSQL\16\data`

- Rode `ipconfig` no cmd e pegue o IP da sua máquina, geralmente é o que começa com `192.168.x.x` assim como o de ethernet. e coloque no arquivo `pg_hba.conf` que geralmente está localizado em `C:\Program Files\PostgreSQL\16\data` e adicione a linha `host    all             all             192.168.x.x/24          md5` para todos esses IPs.

- Caso esteja com problemas, abra o Windows Defender Firewall with Advanced Security. Clique em Inbound Rules e New Rule. Selecione Port e clique em Next. Selecione TCP e coloque o número da porta que o PostgreSQL está rodando, geralmente é 5432. Clique em Next e selecione Allow the connection. Clique em Next e selecione Domain, Private e Public. Clique em Next e coloque um nome para a regra. Clique em Finish.

- Reinicie o PostgreSQL através dos comandos `net stop postgresql-x64-16` e `net start postgresql-x64-16` no cmd.

- Rode o `q1.py` e veja se ele funciona.

Se der errado, fala comigo (Otávio)
