# Roteiro de execução no Databricks

Este roteiro leva do arquivo baixado até as tabelas e evidências do MVP. A
execução deve ser feita no Databricks Free Edition, porque o trabalho exige uma
plataforma em nuvem.

## 1. Baixar somente a fonte oficial

Baixe a base **Brazilian E-Commerce Public Dataset by Olist**:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

O projeto usa quatro arquivos:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`

Não renomeie os arquivos. Não coloque os CSVs no GitHub. A página do conjunto
informa licença CC BY-NC-SA 4.0; por isso, este uso fica restrito ao MVP
acadêmico, com atribuição da fonte e sem uso comercial.

## 2. Importar o notebook

No workspace do Databricks, importe `01_pipeline_olist.py`. Arquivos exportados
no formato Databricks Source mantêm as divisões de célula indicadas por
`# COMMAND ----------`.

Abra o notebook e confirme os parâmetros no topo:

- catálogo: `workspace`
- schema: `mvp_olist_bruno`
- volume: `dados`

Se o seu ambiente apresentar outro catálogo padrão, altere somente o parâmetro
`catalogo`. O restante do notebook usa esse valor automaticamente.

## 3. Criar o local de entrada

Execute as células até aparecer a mensagem com a pasta de entrada. O notebook
cria um volume gerenciado e usa o caminho:

`/Volumes/workspace/mvp_olist_bruno/dados/origem`

No Catalog Explorer, abra o catálogo, o schema e o volume criados. Crie a pasta
`origem` e envie os quatro CSVs. O Databricks recomenda volumes para arquivos
de ingestão e usa o padrão `/Volumes/<catalogo>/<schema>/<volume>/...`.

Documentação oficial:

- https://docs.databricks.com/aws/en/volumes/
- https://docs.databricks.com/aws/en/volumes/volume-files
- https://docs.databricks.com/aws/en/volumes/paths

## 4. Executar o pipeline

Depois do envio, execute o notebook inteiro. A sequência é:

1. Bronze: lê os quatro CSVs como texto e registra arquivo, data e lote.
2. Silver: converte tipos, cria indicadores de qualidade e agrega relações
   um-para-muitos por pedido.
3. Gold: reúne uma linha por pedido e calcula atraso, faixas e elegibilidade.
4. Respostas: cria tabelas agregadas para as três perguntas do MVP e um recorte
   complementar sobre o momento da avaliação.
5. Qualidade: cria uma tabela com testes, exceções e tamanho das amostras.

A carga usa `overwrite` porque a fonte é estática. Isso permite repetir o
notebook sem acumular cópias das mesmas linhas.

## 5. Conferências obrigatórias

No resultado de `gold_controle_qualidade`, confira principalmente:

- `pedidos_lidos_na_bronze` igual a `pedidos_na_gold`;
- `order_id_duplicado_na_gold` igual a zero;
- quantidade de pedidos sem itens, pagamento ou avaliação;
- quantidade de cronologias inválidas;
- quantidade elegível para a análise principal.

O notebook interrompe a execução se a Gold perder ou multiplicar pedidos. As
demais ocorrências são mostradas como atenção ou informação, pois nem toda
ausência é um erro da base.

## 6. Consultas rápidas para inspeção

Use uma célula SQL para verificar as tabelas:

```sql
SHOW TABLES IN workspace.mvp_olist_bruno;
```

```sql
SELECT *
FROM workspace.mvp_olist_bruno.gold_controle_qualidade
ORDER BY status, controle;
```

```sql
SELECT *
FROM workspace.mvp_olist_bruno.gold_avaliacao_por_atraso;
```

Troque `workspace` caso tenha usado outro catálogo.

## 7. Evidências para o README

Faça capturas legíveis, sem mostrar informações pessoais da conta. Salve as
imagens em `docs/evidencias` e incorpore-as ao README principal:

1. notebook aberto no Databricks, com o nome do projeto;
2. Catalog Explorer mostrando uma tabela Gold, suas colunas e descrições;
3. consulta ou tela mostrando as tabelas Bronze, Silver e Gold persistidas;
4. resultado da tabela `gold_controle_qualidade`;
5. resultado de `gold_avaliacao_por_atraso`;
6. resultado de `gold_avaliacao_por_atraso_valor`;
7. resultado de `gold_avaliacao_por_atraso_pagamento`;
8. resultado de `gold_avaliacao_cartao_parcelas`;
9. resultado de `gold_avaliacao_por_atraso_momento`, quando utilizado como
   evidência complementar da limitação temporal das avaliações.

Para cada captura, anote em uma frase o que ela comprova. Exemplo: “A tabela
Gold manteve uma linha por pedido após a consolidação de itens, pagamentos e
avaliações.”

## 8. O que ainda depende da execução

As conclusões não devem ser escritas antes de rodar o notebook. Depois da
execução, registre para cada pergunta:

- tamanho da amostra;
- diferença de nota média e de percentual negativo entre os grupos;
- grupos com poucos pedidos;
- possíveis explicações alternativas;
- limite da análise: associação observada, sem afirmar causalidade.

## 9. Organização do GitHub

Estrutura inicial sugerida:

```text
mvp-engenharia-dados-olist/
├── README.md
├── notebooks/
│   └── 01_pipeline_olist.py
├── docs/
│   ├── catalogo_dados.md
│   └── evidencias/
└── .gitignore
```

Inclua os nomes dos CSVs no `.gitignore`. O README deve citar a fonte, a
licença, as transformações e explicar que os dados brutos não são redistribuídos.
