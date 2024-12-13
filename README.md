                        PROJETO INTEGRADOR IV - CURSO BIG DATA PARA NEGÓCIOS / FATEC IPIRANGA

-----------------------------------------------------------------------

                                            Cliente / Empresa:
                                                        
                            Razão Social: LUCIVAL ALVES DOS SANTOS 58387862568
                            Nome Fantasia: Translog Pombinha Branca
                            Porte: Microempresa
                            CNPJ: 09.446.165/0001-66
                            Data de Abertura: 06/05/2020 
                            Site: https://www.translogpombinhabranca.com/	
                            Responsável: Lúcio Santos
                            Email: lucio.santos@translogpombinhabranca.com
                            Atividade: Transporte Rodoviário de Carga
                                    
-----------------------------------------------------------------------                                
                                             DELTA LAKEHOUSE e DATA LAKE
                        
No projeto Translog Pombinha Branca, a escolha das ferramentas de
armazenamento e processamento de dados foi baseada em critérios como
escalabilidade, flexibilidade, custo-benefício e integração com tecnologias analíticas
modernas. A combinação de um Data Lake na AWS S3, um Data Lakehouse e a
plataforma Databricks oferece uma infraestrutura robusta para atender às necessidades
de coleta, organização, transformação e análise dos dados.

### **Arquitetura:**
A imagem anexada é uma representação arquitetural de um Data Lakehouse baseado no Databricks e Apache Spark, com integração ao Delta Lake para gerenciar dados de forma escalável e confiável. Vou detalhar os elementos presentes:

<img align="center" src="https://github.com/WilPassion/ProjetoIntegrador4_FATEC/blob/main/imgs/arquitetura-datalakehouse.png" alt="arquitetura" width="1000">   

* Delta Lake: Gerencia a consistência dos dados através de versões e transações ACID, essencial para evitar problemas de concorrência e garantir integridade.

* Apache Spark: Responsável pelo processamento distribuído dos dados, possibilitando análises rápidas em grandes volumes.

* Databricks Workflows: Automatizam as tarefas de ingestão, transformação e geração de relatórios, garantindo que o pipeline funcione de ponta a ponta.
  
### **Workflow "Upsell" no Databricks:**

O workflow Upsell implementado no Databricks gerencia a transformação e o processamento de dados em múltiplos estágios, organizados em camadas bronze, silver e gold.

* Bronze: Realiza a ingestão inicial de dados brutos, como preços de combustíveis e informações de viagens.

* Silver: Normaliza e limpa os dados, preparando-os para análises mais detalhadas.

* Gold: Gera relatórios e previsões, como o forecast de diesel e relatórios consolidados diários, mensais e gerais.

Esse pipeline automatizado garante a confiabilidade e a eficiência no tratamento dos dados, com controle de versões integrados ao repositório no GitHub.


-----------------------------------------------------------------------                                
                                             MACHINE LEARNING - MODELO ARIMA - FORECAST DIESEL
                                             
Para análise preditiva de preços de combustíveis, foi desenvolvido um modelo utilizando o algoritmo ARIMA (AutoRegressive Integrated Moving Average). Esse modelo foi implementado com o objetivo de prever tendências de preços futuros com base em séries históricas. A fonte dos dados utilizados foi a [Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP)](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis), que disponibiliza séries históricas de preços de combustíveis por meio de sua plataforma de dados abertos.

O modelo testado no Projeto Integrador conseguiu gerar o seguinte resultado abaixo para o combustível do tipo Diesel:

<img align="center" src="https://github.com/WilPassion/ProjetoIntegrador4_FATEC/blob/main/imgs/ARIMA.PNG" alt="arima" width="1000"> 
Baseando-se no histórico de preços reportados, o modelo conseguiu ser satisfatório ao gerar padrões que sinalizam tendências de aumento de preço em dias determinados. 
                                             
-----------------------------------------------------------------------                                
                                             DASHBOARDS

Para a análise de métricas e geração de insights do projeto, foi escolhida a ferramenta Power BI. Os dados utilizados foram modificados para proteger a confidencialidade das informações da empresa.     

Para checagem completa [clicar aqui](https://app.powerbi.com/view?r=eyJrIjoiNmNlMjMyNzAtYTJlMC00YzkxLWFjMDgtZjJjY2FlYTI2NGQzIiwidCI6ImNmNzJlMmJkLTdhMmItNDc4My1iZGViLTM5ZDU3YjA3Zjc2ZiIsImMiOjR9) 

### **Cover:**
<img align="center" src="https://github.com/WilPassion/ProjetoIntegrador4_FATEC/blob/main/power-bi/cover.PNG" alt="cover-dashes" width="1000"> 
  
### **Relatório Financeiro:**
<img align="center" src="https://github.com/WilPassion/ProjetoIntegrador4_FATEC/blob/main/power-bi/relatorio_financeiro.PNG" alt="dash_financeiro" width="1000">  
* Métricas:

Receita Total
Custo com Combustível
Lucro Total
Viagens Realizadas
Margem Líquida
Distribuição de Coletas por Cidade
Lucro por Mês

### **Relatório Consumo:**
<img align="center" src="https://github.com/WilPassion/ProjetoIntegrador4_FATEC/blob/main/power-bi/relatorio_consumo.PNG" alt="dash_consumo" width="1000"> 
* Métricas:

Custo com Combustível850
Média de Combustível por Viagem
Litros Consumidos
Custo de Combustível por Mês
Distribuição de Coletas por Cidade
Previsão Diesel (valores gerados pelo modelo ARIMA citado acima)
