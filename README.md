# eng_dados-big_data
Projeto final proposto no módulo VI (Big Data) da trilha de Engenharia de Dados (Santander Coders 2023)

**Proposta de Projeto: Desenvolvimento e Avaliação de uma Arquitetura Distribuída para o Cadastro Ambiental Rural no Databricks**

**1. Título do Projeto:**  
   Desenvolvimento e Avaliação de uma Arquitetura Distribuída para o Cadastro Ambiental Rural no Databricks.

**2. Objetivo:**  
   O objetivo deste projeto é explorar as capacidades de arquiteturas de bancos de dados distribuídos utilizando a plataforma Databricks, focando no "Cadastro Ambiental Rural". A proposta envolve a criação, implementação e avaliação de uma arquitetura distribuída, incluindo migração de dados e testes comparativos.

**3. Descrição do Problema:**  
   O "Cadastro Ambiental Rural" é um extenso conjunto de dados sobre propriedades rurais no Brasil. O desafio é criar uma arquitetura que permita o gerenciamento eficiente dessas informações para consultas e análises rápidas, considerando o tamanho considerável da base de dados.

**4. Tarefas:**

- **Proposição da Nova Arquitetura:**  
  - Escolha o Databricks como plataforma principal.
  - Utilize o Delta Lake para garantir consistência transacional e controle de versão.
  - Implemente uma arquitetura distribuída utilizando Apache Spark para processamento paralelo.

- **Implementação da Arquitetura:**  
  - Configure e integre sistemas de gerenciamento de banco de dados distribuído no ambiente Databricks.
  - Realize a integração de servidores e clusters necessários para suportar a carga de trabalho.

- **Modelagem do Novo Banco de Dados:**  
  - Modele os dados do "Cadastro Ambiental Rural" conforme a arquitetura proposta, utilizando Delta Lake para otimizar a manipulação dos dados.

- **Projeto de Particionamento Adequado/Efetivo:**  
  - Desenvolva um esquema de particionamento horizontal considerando estratégias eficientes para distribuição dos dados nos nós do banco de dados distribuído.

- **Bateria de Testes (Queries):**  
  - Realize testes comparativos de consultas no ambiente distribuído e centralizado.
  - Avalie desempenho, tempo de resposta, escalabilidade e consumo de recursos.
  - Registre e analise os resultados para fornecer insights sobre a eficácia da arquitetura proposta.

**5. Consultas:**

1. **Consulta 1:**  
   Recupere a soma de área (em hectares) para todas as propriedades agrícolas que pertencem ao MS e MT. Ordene os resultados em ordem decrescente. Essa consulta visa identificar a extensão total das propriedades agrícolas nos estados de Mato Grosso (MT) e Mato Grosso do Sul (MS), apresentando os resultados de forma decrescente.

2. **Consulta 2:**  
   Filtre todas as propriedades que pertencem à região sudeste. Esta consulta visa extrair informações específicas das propriedades localizadas na região Sudeste do Brasil, proporcionando uma visão detalhada dessas áreas.

3. **Consulta 3:**  
   Recupere todas as propriedades rurais que estão localizadas dentro de uma área geográfica específica delimitada por um polígono. Esta consulta tem como objetivo identificar todas as propriedades rurais situadas dentro de um polígono descrito pelas seguintes coordenadas:  ((-53.5325072 -19.4632582, -51.0495971 -19.1625841, -51.3734501 -16.1924262, -53.8181518 -16.4010783, -53.5325072 -19.4632582))

4. **Consulta 4:**  
   Calcule quantas propriedades foram cadastradas por ano. Apresente os resultados em ordem cronológica. Essa consulta tem o propósito de fornecer uma contagem anual do número de propriedades cadastradas, apresentando os resultados em ordem cronológica.

5. **Consulta 5:**  
   Calcule o percentual médio de área remanescente de vegetação nativa em comparação à área total da propriedade. Esta consulta busca calcular o percentual médio da área de vegetação nativa que permanece em relação à área total das propriedades rurais, oferecendo insights sobre a preservação ambiental.

6. **Consulta 6:**  
   Construa uma consulta que mostre a contagem de propriedades rurais por estado. Esta consulta visa fornecer uma visão consolidada do número de propriedades rurais agrupadas por estado, oferecendo uma análise geográfica da distribuição das propriedades.

7. **Consulta 7:**  
   Veja qual é a maior propriedade entre todas e calcule a distância entre ela e Brasília. Esta consulta tem como objetivo identificar a maior propriedade rural e calcular a distância entre o seu centro e Brasília, utilizando coordenadas geográficas para essa avaliação.

8. **Consulta 8:**  
   Faça a média de área entre todas as propriedades. Calcule quantas propriedades por estado estão acima da média. Nesta consulta, busca-se calcular a média da área total das propriedades e, em seguida, determinar quantas propriedades por estado estão acima dessa média, proporcionando insights sobre áreas com tamanhos superiores à média nacional.
