# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # MVP de Engenharia de Dados — atraso, pagamento e avaliação
# MAGIC
# MAGIC Este notebook monta um pipeline simples e rastreável com dados públicos da Olist.
# MAGIC A ideia é medir se o atraso na entrega está associado à avaliação do cliente e observar
# MAGIC se o valor do pedido, a forma de pagamento e o parcelamento mudam essa relação.
# MAGIC
# MAGIC **Fonte:** Olist Brazilian E-Commerce Public Dataset, disponível no Kaggle.
# MAGIC **Licença:** CC BY-NC-SA 4.0. Uso restrito a este MVP acadêmico, sem finalidade comercial.
# MAGIC Os CSVs originais não devem ser publicados no repositório.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração
# MAGIC
# MAGIC Primeiro crio um schema e um volume. Depois desta célula, os quatro CSVs devem ser
# MAGIC enviados para a pasta `origem` do volume, conforme o roteiro de execução.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema", "mvp_olist_bruno", "Schema")
dbutils.widgets.text("volume", "dados", "Volume")

catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip()
volume = dbutils.widgets.get("volume").strip()

for nome, valor in {"catalogo": catalogo, "schema": schema, "volume": volume}.items():
    if not valor or "`" in valor or "/" in valor:
        raise ValueError(f"Valor inválido para {nome}: {valor!r}")

namespace = f"`{catalogo}`.`{schema}`"
volume_name = f"{namespace}.`{volume}`"
origem = f"/Volumes/{catalogo}/{schema}/{volume}/origem"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")
spark.sql(f"USE CATALOG `{catalogo}`")
spark.sql(f"USE SCHEMA `{schema}`")
dbutils.fs.mkdirs(origem)

print(f"Pasta de entrada: {origem}")

# COMMAND ----------

# MAGIC %md
# MAGIC Arquivos esperados:
# MAGIC
# MAGIC - `olist_orders_dataset.csv`
# MAGIC - `olist_order_items_dataset.csv`
# MAGIC - `olist_order_payments_dataset.csv`
# MAGIC - `olist_order_reviews_dataset.csv`

# COMMAND ----------

arquivos = {
    "pedidos": "olist_orders_dataset.csv",
    "itens": "olist_order_items_dataset.csv",
    "pagamentos": "olist_order_payments_dataset.csv",
    "avaliacoes": "olist_order_reviews_dataset.csv",
}

arquivos_encontrados = {item.name for item in dbutils.fs.ls(origem)}
arquivos_ausentes = [nome for nome in arquivos.values() if nome not in arquivos_encontrados]

if arquivos_ausentes:
    raise FileNotFoundError(
        "Envie os quatro CSVs para a pasta de entrada antes de continuar. "
        f"Faltando: {', '.join(arquivos_ausentes)}"
    )

print("Arquivos de origem encontrados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze — cópia fiel da origem
# MAGIC
# MAGIC Na Bronze, todas as colunas permanecem como texto. Acrescento apenas dados de controle
# MAGIC da carga. Como a fonte é um retrato estático, a execução substitui a carga anterior.

# COMMAND ----------

lote_ingestao = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def nome_tabela(nome: str) -> str:
    return f"{catalogo}.{schema}.{nome}"


def gravar_tabela(df: DataFrame, nome: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nome_tabela(nome))
    )


def carregar_bronze(chave: str, tabela: str) -> DataFrame:
    arquivo = arquivos[chave]
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(f"{origem}/{arquivo}")
        .withColumn("_arquivo_origem", F.lit(arquivo))
        .withColumn("_data_ingestao", F.current_timestamp())
        .withColumn("_lote_ingestao", F.lit(lote_ingestao))
    )
    gravar_tabela(df, tabela)
    return df


bronze_pedidos = carregar_bronze("pedidos", "bronze_pedidos")
bronze_itens = carregar_bronze("itens", "bronze_itens")
bronze_pagamentos = carregar_bronze("pagamentos", "bronze_pagamentos")
bronze_avaliacoes = carregar_bronze("avaliacoes", "bronze_avaliacoes")

print("Camada Bronze concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver — tipos, regras e consolidações
# MAGIC
# MAGIC A Silver converte os tipos e registra problemas sem esconder linhas. Itens, pagamentos
# MAGIC e avaliações são consolidados por pedido antes das junções. Assim, a tabela final não
# MAGIC multiplica valores por causa de relações um-para-muitos.

# COMMAND ----------

silver_pedidos = (
    bronze_pedidos.select(
        "order_id",
        "customer_id",
        "order_status",
        F.to_timestamp("order_purchase_timestamp").alias("data_compra"),
        F.to_timestamp("order_approved_at").alias("data_aprovacao"),
        F.to_timestamp("order_delivered_carrier_date").alias("data_entrega_transportadora"),
        F.to_timestamp("order_delivered_customer_date").alias("data_entrega_cliente"),
        F.to_timestamp("order_estimated_delivery_date").alias("data_entrega_estimada"),
        "_arquivo_origem",
        "_data_ingestao",
        "_lote_ingestao",
    )
    .withColumn("flag_status_entregue", F.col("order_status") == "delivered")
    .withColumn("flag_data_entrega_ausente", F.col("data_entrega_cliente").isNull())
    .withColumn("flag_data_estimada_ausente", F.col("data_entrega_estimada").isNull())
    .withColumn(
        "flag_cronologia_invalida",
        (F.col("data_entrega_cliente") < F.col("data_compra"))
        | (
            F.col("data_entrega_transportadora").isNotNull()
            & (F.col("data_entrega_cliente") < F.col("data_entrega_transportadora"))
        ),
    )
    .withColumn(
        "flag_elegivel_atraso",
        F.col("flag_status_entregue")
        & F.col("data_compra").isNotNull()
        & F.col("data_entrega_cliente").isNotNull()
        & F.col("data_entrega_estimada").isNotNull()
        & ~F.coalesce(F.col("flag_cronologia_invalida"), F.lit(False)),
    )
)

gravar_tabela(silver_pedidos, "silver_pedidos")

# COMMAND ----------

silver_itens = (
    bronze_itens.select(
        "order_id",
        F.col("order_item_id").cast("int").alias("numero_item"),
        "product_id",
        "seller_id",
        F.to_timestamp("shipping_limit_date").alias("data_limite_envio"),
        F.col("price").cast("decimal(18,2)").alias("valor_produto"),
        F.col("freight_value").cast("decimal(18,2)").alias("valor_frete"),
    )
    .withColumn(
        "flag_item_invalido",
        F.col("numero_item").isNull()
        | F.col("valor_produto").isNull()
        | F.col("valor_frete").isNull()
        | (F.col("valor_produto") < 0)
        | (F.col("valor_frete") < 0),
    )
)

silver_itens_por_pedido = (
    silver_itens.groupBy("order_id")
    .agg(
        F.count("*").alias("quantidade_itens"),
        F.sum("valor_produto").cast("decimal(18,2)").alias("valor_produtos"),
        F.sum("valor_frete").cast("decimal(18,2)").alias("valor_frete"),
        F.max(F.col("flag_item_invalido").cast("int")).alias("tem_item_invalido"),
    )
    .withColumn("valor_compra", F.col("valor_produtos"))
    .withColumn(
        "valor_total_itens_frete",
        (F.col("valor_produtos") + F.col("valor_frete")).cast("decimal(18,2)"),
    )
    .withColumn("flag_item_valido", F.col("tem_item_invalido") == 0)
)

gravar_tabela(silver_itens, "silver_itens")
gravar_tabela(silver_itens_por_pedido, "silver_itens_por_pedido")

# COMMAND ----------

silver_pagamentos = (
    bronze_pagamentos.select(
        "order_id",
        F.col("payment_sequential").cast("int").alias("sequencia_pagamento"),
        F.trim("payment_type").alias("tipo_pagamento"),
        F.col("payment_installments").cast("int").alias("parcelas"),
        F.col("payment_value").cast("decimal(18,2)").alias("valor_pagamento"),
    )
    .withColumn(
        "flag_pagamento_invalido",
        F.col("sequencia_pagamento").isNull()
        | F.col("tipo_pagamento").isNull()
        | (F.col("tipo_pagamento") == "")
        | F.col("parcelas").isNull()
        | F.col("valor_pagamento").isNull()
        | (F.col("valor_pagamento") <= 0),
    )
)

silver_pagamentos_por_pedido = (
    silver_pagamentos.groupBy("order_id")
    .agg(
        F.count("*").alias("quantidade_pagamentos"),
        F.sum("valor_pagamento").cast("decimal(18,2)").alias("valor_total_pagamento"),
        F.sort_array(F.collect_set("tipo_pagamento")).alias("tipos_pagamento"),
        F.max(F.col("flag_pagamento_invalido").cast("int")).alias("tem_pagamento_invalido"),
        F.countDistinct(
            F.when(F.col("tipo_pagamento") == "credit_card", F.col("parcelas"))
        ).alias("quantidade_planos_cartao"),
        F.max(
            F.when(F.col("tipo_pagamento") == "credit_card", F.col("parcelas"))
        ).alias("maior_numero_parcelas_cartao"),
    )
    .withColumn("quantidade_tipos_pagamento", F.size("tipos_pagamento"))
    .withColumn("combinacao_pagamento", F.concat_ws(" + ", "tipos_pagamento"))
    .withColumn(
        "modalidade_pagamento",
        F.when(F.col("quantidade_tipos_pagamento") > 1, "combinado")
        .when(F.col("quantidade_tipos_pagamento") == 1, F.element_at("tipos_pagamento", 1))
        .otherwise("sem_pagamento"),
    )
    .withColumn("flag_pagamento_valido", F.col("tem_pagamento_invalido") == 0)
    .withColumn(
        "grupo_parcelamento_cartao",
        F.when(F.col("modalidade_pagamento") != "credit_card", "nao_aplicavel")
        .when(F.col("quantidade_planos_cartao") != 1, "plano_inconsistente")
        .when(F.col("maior_numero_parcelas_cartao") <= 0, "parcelas_invalidas")
        .when(F.col("maior_numero_parcelas_cartao") == 1, "1_parcela")
        .otherwise("2_ou_mais"),
    )
)

gravar_tabela(silver_pagamentos, "silver_pagamentos")
gravar_tabela(silver_pagamentos_por_pedido, "silver_pagamentos_por_pedido")

# COMMAND ----------

silver_avaliacoes = (
    bronze_avaliacoes.select(
        "review_id",
        "order_id",
        F.col("review_score").cast("int").alias("nota_avaliacao"),
        "review_comment_title",
        "review_comment_message",
        F.to_timestamp("review_creation_date").alias("data_criacao_avaliacao"),
        F.to_timestamp("review_answer_timestamp").alias("data_resposta_avaliacao"),
    )
    .withColumn(
        "flag_avaliacao_valida",
        F.col("nota_avaliacao").between(1, 5),
    )
)

avaliacoes_validas = silver_avaliacoes.filter("flag_avaliacao_valida")

janela_primeira = Window.partitionBy("order_id").orderBy(
    F.col("data_resposta_avaliacao").asc_nulls_last(),
    F.col("data_criacao_avaliacao").asc_nulls_last(),
    F.col("review_id").asc(),
)
janela_ultima = Window.partitionBy("order_id").orderBy(
    F.col("data_resposta_avaliacao").desc_nulls_last(),
    F.col("data_criacao_avaliacao").desc_nulls_last(),
    F.col("review_id").desc(),
)

primeira_avaliacao = (
    avaliacoes_validas.withColumn("ordem", F.row_number().over(janela_primeira))
    .filter("ordem = 1")
    .select(
        "order_id",
        F.col("nota_avaliacao").alias("primeira_nota_avaliacao"),
        F.col("data_resposta_avaliacao").alias("data_primeira_avaliacao"),
    )
)

ultima_avaliacao = (
    avaliacoes_validas.withColumn("ordem", F.row_number().over(janela_ultima))
    .filter("ordem = 1")
    .select(
        "order_id",
        "review_id",
        F.col("nota_avaliacao").alias("nota_avaliacao"),
        "review_comment_title",
        "review_comment_message",
        F.col("data_criacao_avaliacao").alias("data_criacao_avaliacao"),
        F.col("data_resposta_avaliacao").alias("data_resposta_avaliacao"),
    )
)

contagem_avaliacoes = avaliacoes_validas.groupBy("order_id").agg(
    F.count("*").alias("quantidade_avaliacoes_validas")
)

silver_avaliacoes_por_pedido = (
    ultima_avaliacao.join(primeira_avaliacao, "order_id", "left")
    .join(contagem_avaliacoes, "order_id", "left")
    .withColumn(
        "classe_avaliacao",
        F.when(F.col("nota_avaliacao").isin(1, 2), "negativa")
        .when(F.col("nota_avaliacao") == 3, "neutra")
        .when(F.col("nota_avaliacao").isin(4, 5), "positiva"),
    )
    .withColumn(
        "flag_nota_alterada",
        (F.col("quantidade_avaliacoes_validas") > 1)
        & (F.col("primeira_nota_avaliacao") != F.col("nota_avaliacao")),
    )
)

gravar_tabela(silver_avaliacoes, "silver_avaliacoes")
gravar_tabela(silver_avaliacoes_por_pedido, "silver_avaliacoes_por_pedido")

print("Camada Silver concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold — uma linha por pedido
# MAGIC
# MAGIC A avaliação escolhida é a última resposta válida de cada pedido. A primeira nota também
# MAGIC fica registrada para sabermos quantos casos mudaram. A nota 3 é tratada como neutra.

# COMMAND ----------

gold_pedidos = (
    silver_pedidos.join(silver_itens_por_pedido, "order_id", "left")
    .join(silver_pagamentos_por_pedido, "order_id", "left")
    .join(silver_avaliacoes_por_pedido, "order_id", "left")
    .withColumn(
        "dias_atraso",
        F.when(
            F.col("flag_elegivel_atraso"),
            F.datediff(F.to_date("data_entrega_cliente"), F.to_date("data_entrega_estimada")),
        ),
    )
    .withColumn(
        "faixa_atraso",
        F.when(~F.col("flag_elegivel_atraso"), "nao_elegivel")
        .when(F.col("dias_atraso") <= 0, "no_prazo")
        .when(F.col("dias_atraso") <= 3, "1_a_3_dias")
        .when(F.col("dias_atraso") <= 7, "4_a_7_dias")
        .when(F.col("dias_atraso") <= 14, "8_a_14_dias")
        .otherwise("15_dias_ou_mais"),
    )
    .withColumn(
        "ordem_faixa_atraso",
        F.when(F.col("faixa_atraso") == "no_prazo", 0)
        .when(F.col("faixa_atraso") == "1_a_3_dias", 1)
        .when(F.col("faixa_atraso") == "4_a_7_dias", 2)
        .when(F.col("faixa_atraso") == "8_a_14_dias", 3)
        .when(F.col("faixa_atraso") == "15_dias_ou_mais", 4)
        .otherwise(9),
    )
    .withColumn("flag_atrasado", F.col("dias_atraso") > 0)
    .withColumn(
        "faixa_valor_pedido",
        F.when(F.col("valor_compra").isNull(), "sem_valor")
        .when(F.col("valor_compra") <= 50, "ate_50")
        .when(F.col("valor_compra") <= 100, "50_01_a_100")
        .when(F.col("valor_compra") <= 200, "100_01_a_200")
        .otherwise("acima_de_200"),
    )
    .withColumn(
        "ordem_faixa_valor",
        F.when(F.col("faixa_valor_pedido") == "ate_50", 0)
        .when(F.col("faixa_valor_pedido") == "50_01_a_100", 1)
        .when(F.col("faixa_valor_pedido") == "100_01_a_200", 2)
        .when(F.col("faixa_valor_pedido") == "acima_de_200", 3)
        .otherwise(9),
    )
    .withColumn(
        "diferenca_valor_pagamento_item",
        (F.col("valor_total_pagamento") - F.col("valor_total_itens_frete")).cast("decimal(18,2)"),
    )
    .withColumn(
        "flag_divergencia_valores",
        F.col("diferenca_valor_pagamento_item").isNotNull()
        & (F.abs(F.col("diferenca_valor_pagamento_item")) > F.lit(0.01)),
    )
    .withColumn("flag_sem_itens", F.col("quantidade_itens").isNull())
    .withColumn("flag_sem_pagamento", F.col("quantidade_pagamentos").isNull())
    .withColumn("flag_sem_avaliacao", F.col("nota_avaliacao").isNull())
    .withColumn(
        "flag_avaliacao_antes_entrega",
        F.col("data_resposta_avaliacao").isNotNull()
        & F.col("data_entrega_cliente").isNotNull()
        & (F.col("data_resposta_avaliacao") < F.col("data_entrega_cliente")),
    )
    .withColumn(
        "momento_resposta_avaliacao",
        F.when(F.col("data_resposta_avaliacao").isNull(), "sem_data_resposta")
        .when(F.col("flag_avaliacao_antes_entrega"), "antes_da_entrega")
        .otherwise("na_entrega_ou_depois"),
    )
    .withColumn(
        "flag_elegivel_analise_principal",
        F.col("flag_elegivel_atraso") & F.col("nota_avaliacao").isNotNull(),
    )
)

gravar_tabela(gold_pedidos, "gold_pedidos_analitico")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tabelas de resposta
# MAGIC
# MAGIC Cada saída mostra a quantidade de pedidos, a nota média e a participação de avaliações
# MAGIC negativas, neutras e positivas. Os filtros mudam conforme a pergunta para não usar
# MAGIC informação ausente no recorte.

# COMMAND ----------

def resumir(df: DataFrame, dimensoes: list[str]) -> DataFrame:
    return (
        df.groupBy(*dimensoes)
        .agg(
            F.count("*").alias("quantidade_pedidos"),
            F.round(F.avg("nota_avaliacao"), 3).alias("nota_media"),
            F.sum(F.when(F.col("classe_avaliacao") == "negativa", 1).otherwise(0)).alias("avaliacoes_negativas"),
            F.sum(F.when(F.col("classe_avaliacao") == "neutra", 1).otherwise(0)).alias("avaliacoes_neutras"),
            F.sum(F.when(F.col("classe_avaliacao") == "positiva", 1).otherwise(0)).alias("avaliacoes_positivas"),
        )
        .withColumn(
            "percentual_negativas",
            F.round(100 * F.col("avaliacoes_negativas") / F.col("quantidade_pedidos"), 2),
        )
        .withColumn(
            "percentual_neutras",
            F.round(100 * F.col("avaliacoes_neutras") / F.col("quantidade_pedidos"), 2),
        )
        .withColumn(
            "percentual_positivas",
            F.round(100 * F.col("avaliacoes_positivas") / F.col("quantidade_pedidos"), 2),
        )
        .withColumn("flag_grupo_pequeno", F.col("quantidade_pedidos") < 30)
    )


base_principal = gold_pedidos.filter("flag_elegivel_analise_principal")

gold_avaliacao_por_atraso = resumir(
    base_principal,
    ["ordem_faixa_atraso", "faixa_atraso"],
)

gold_avaliacao_por_atraso_valor = resumir(
    base_principal.filter("flag_item_valido = true"),
    ["ordem_faixa_atraso", "faixa_atraso", "ordem_faixa_valor", "faixa_valor_pedido"],
)

gold_avaliacao_por_atraso_pagamento = resumir(
    base_principal.filter("flag_pagamento_valido = true"),
    ["ordem_faixa_atraso", "faixa_atraso", "modalidade_pagamento"],
)

gold_avaliacao_cartao_parcelas = resumir(
    base_principal.filter(
        "flag_pagamento_valido = true "
        "AND modalidade_pagamento = 'credit_card' "
        "AND grupo_parcelamento_cartao IN ('1_parcela', '2_ou_mais')"
    ),
    ["ordem_faixa_atraso", "faixa_atraso", "grupo_parcelamento_cartao"],
)

gold_avaliacao_por_atraso_momento = resumir(
    base_principal,
    ["ordem_faixa_atraso", "faixa_atraso", "momento_resposta_avaliacao"],
)

gravar_tabela(gold_avaliacao_por_atraso, "gold_avaliacao_por_atraso")
gravar_tabela(gold_avaliacao_por_atraso_valor, "gold_avaliacao_por_atraso_valor")
gravar_tabela(gold_avaliacao_por_atraso_pagamento, "gold_avaliacao_por_atraso_pagamento")
gravar_tabela(gold_avaliacao_cartao_parcelas, "gold_avaliacao_cartao_parcelas")
gravar_tabela(gold_avaliacao_por_atraso_momento, "gold_avaliacao_por_atraso_momento")

print("Camada Gold concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Controle de qualidade
# MAGIC
# MAGIC Algumas ocorrências são erros; outras apenas explicam a redução da amostra. As duas
# MAGIC coisas ficam separadas para a análise não parecer mais completa do que realmente é.

# COMMAND ----------

def contar(df: DataFrame, condicao: str | None = None) -> int:
    return df.filter(condicao).count() if condicao else df.count()


total_bronze_pedidos = contar(bronze_pedidos)
total_gold = contar(gold_pedidos)
duplicados_bronze = (
    bronze_pedidos.groupBy("order_id").count().filter("count > 1").count()
)
duplicados_gold = gold_pedidos.groupBy("order_id").count().filter("count > 1").count()

controles = [
    ("pedidos_lidos_na_bronze", total_bronze_pedidos, "INFORMATIVO", "Volume recebido da origem"),
    ("pedidos_na_gold", total_gold, "INFORMATIVO", "Deve manter uma linha por pedido"),
    ("order_id_duplicado_na_bronze", duplicados_bronze, "OK" if duplicados_bronze == 0 else "ERRO", "Chave esperada como única"),
    ("order_id_duplicado_na_gold", duplicados_gold, "OK" if duplicados_gold == 0 else "ERRO", "Junções não podem multiplicar pedidos"),
    ("itens_invalidos", contar(silver_itens, "flag_item_invalido"), "ATENCAO", "Conversão ou valor negativo"),
    ("pagamentos_invalidos", contar(silver_pagamentos, "flag_pagamento_invalido"), "ATENCAO", "Conversão, campo vazio ou valor não positivo"),
    ("avaliacoes_invalidas", contar(silver_avaliacoes, "COALESCE(flag_avaliacao_valida, false) = false"), "ATENCAO", "Nota fora da escala de 1 a 5"),
    ("cronologia_invalida", contar(silver_pedidos, "flag_cronologia_invalida"), "ATENCAO", "Entrega anterior à compra ou à transportadora"),
    ("pedidos_sem_itens", contar(gold_pedidos, "flag_sem_itens"), "ATENCAO", "Pedido sem correspondência na base de itens"),
    ("pedidos_sem_pagamento", contar(gold_pedidos, "flag_sem_pagamento"), "ATENCAO", "Pedido sem correspondência na base de pagamentos"),
    ("pedidos_com_divergencia_de_valor", contar(gold_pedidos, "flag_divergencia_valores"), "ATENCAO", "Diferença acima de R$ 0,01 entre itens mais frete e pagamentos"),
    ("pedidos_sem_avaliacao", contar(gold_pedidos, "flag_sem_avaliacao"), "INFORMATIVO", "Reduz a amostra da análise principal"),
    ("pedidos_com_multiplas_avaliacoes", contar(gold_pedidos, "quantidade_avaliacoes_validas > 1"), "INFORMATIVO", "A última resposta válida foi escolhida"),
    ("pedidos_com_nota_alterada", contar(gold_pedidos, "flag_nota_alterada"), "INFORMATIVO", "Primeira e última notas válidas são diferentes"),
    ("avaliacao_respondida_antes_da_entrega", contar(gold_pedidos, "flag_avaliacao_antes_entrega"), "ATENCAO", "Caso para inspeção; não é removido automaticamente"),
    ("pedidos_elegiveis_analise_principal", contar(gold_pedidos, "flag_elegivel_analise_principal"), "INFORMATIVO", "Denominador da pergunta principal"),
]

gold_controle_qualidade = spark.createDataFrame(
    controles,
    "controle string, quantidade long, status string, criterio string",
).withColumn("data_execucao", F.current_timestamp())

gravar_tabela(gold_controle_qualidade, "gold_controle_qualidade")

if total_bronze_pedidos != total_gold or duplicados_gold != 0:
    raise AssertionError("A tabela Gold perdeu ou multiplicou pedidos. Revise as junções.")

display(gold_controle_qualidade.orderBy("status", "controle"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resultados para análise
# MAGIC
# MAGIC Perguntas usadas no MVP:
# MAGIC
# MAGIC 1. Quem compra algo mais caro avalia pior quando a entrega atrasa?
# MAGIC 2. Entre pedidos com atrasos semelhantes, a avaliação muda conforme a forma de pagamento ou o parcelamento?
# MAGIC 3. Nos pedidos entregues no prazo, as avaliações ruins se concentram em alguma dessas características?

# COMMAND ----------

display(gold_avaliacao_por_atraso.orderBy("ordem_faixa_atraso"))

# COMMAND ----------

display(gold_avaliacao_por_atraso_valor.orderBy("ordem_faixa_atraso", "ordem_faixa_valor"))

# COMMAND ----------

display(gold_avaliacao_por_atraso_pagamento.orderBy("ordem_faixa_atraso", "modalidade_pagamento"))

# COMMAND ----------

display(gold_avaliacao_cartao_parcelas.orderBy("ordem_faixa_atraso", "grupo_parcelamento_cartao"))

# COMMAND ----------

# Recorte complementar: o momento da resposta muda bastante a leitura dos atrasos.
display(
    gold_avaliacao_por_atraso_momento.orderBy(
        "ordem_faixa_atraso", "momento_resposta_avaliacao"
    )
)

# COMMAND ----------

# Pergunta 3: olhar apenas a faixa no prazo nas duas características principais.
display(
    gold_avaliacao_por_atraso_valor
    .filter("faixa_atraso = 'no_prazo'")
    .orderBy("ordem_faixa_valor")
)
display(
    gold_avaliacao_por_atraso_pagamento
    .filter("faixa_atraso = 'no_prazo'")
    .orderBy("modalidade_pagamento")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gráficos principais

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1- Avaliações negativas por faixa de atraso

# COMMAND ----------

display(
    spark.table("workspace.mvp_olist_bruno.gold_avaliacao_por_atraso")
         .orderBy("ordem_faixa_atraso")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2- Avaliações negativas por atraso e forma de pagamento

# COMMAND ----------

display(
    spark.table("workspace.mvp_olist_bruno.gold_avaliacao_por_atraso_pagamento")
         .filter("modalidade_pagamento IN ('boleto', 'credit_card')")
         .orderBy("ordem_faixa_atraso", "modalidade_pagamento")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3- Avaliações negativas por atraso e valor do pedido

# COMMAND ----------

display(
    spark.table("workspace.mvp_olist_bruno.gold_avaliacao_por_atraso_valor")
         .orderBy("ordem_faixa_atraso", "ordem_faixa_valor")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4- Avaliações negativas por atraso e parcelamento

# COMMAND ----------

display(
    spark.table("workspace.mvp_olist_bruno.gold_avaliacao_cartao_parcelas")
         .orderBy("ordem_faixa_atraso", "grupo_parcelamento_cartao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fechamento da execução
# MAGIC
# MAGIC Antes de escrever as conclusões, conferir:
# MAGIC
# MAGIC - se a quantidade de pedidos da Bronze e da Gold é a mesma;
# MAGIC - se a Gold continua com `order_id` único;
# MAGIC - quantos pedidos entraram em cada pergunta;
# MAGIC - se grupos pequenos devem ser apenas descritos, sem conclusão forte;
# MAGIC - se os percentuais usam `quantidade_pedidos` como denominador.

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {namespace}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Conclusões e limitações
# MAGIC
# MAGIC O resultado mais claro do trabalho foi a relação entre atraso e avaliação. Entre os pedidos entregues no prazo, 9,26% receberam avaliação negativa. O percentual sobe para 32,13% nos atrasos de 1 a 3 dias e chega a 67,68% entre 4 e 7 dias. Nas faixas mais longas, fica próximo de 80%. Essa evolução não é totalmente linear, já que o grupo de 15 dias ou mais apresentou percentual um pouco menor que o de 8 a 14 dias, mas a diferença entre pedidos no prazo e atrasados é bastante evidente.
# MAGIC
# MAGIC Na análise do valor da compra, os pedidos entregues no prazo acima de R$ 200 tiveram 12,14% de avaliações negativas, contra 7,91% nos pedidos de até R$ 50. Porém, quando o atraso aumenta, essa diferença praticamente desaparece. Portanto, os dados não sustentam que compras mais caras sejam sempre avaliadas pior quando atrasam. Minha leitura é que o próprio atraso pesa mais do que o valor do produto.
# MAGIC
# MAGIC Na comparação das formas de pagamento, boleto e cartão de crédito tiveram praticamente o mesmo resultado nos pedidos entregues no prazo: 9,23% e 9,24% de avaliações negativas. Nos atrasos de 1 a 3 dias e de 4 a 7 dias, o cartão apresentou percentuais maiores. Nos atrasos mais longos, os resultados voltaram a ficar próximos. Isso mostra uma diferença em algumas faixas, mas não permite afirmar que uma forma de pagamento tenha avaliação pior em qualquer situação.
# MAGIC
# MAGIC O parcelamento também não apresentou um comportamento constante. Compras parceladas em duas ou mais vezes tiveram resultado um pouco pior nos pedidos no prazo e nos atrasos curtos, mas essa diferença se inverteu nas faixas mais longas. Assim, não encontrei evidência de que o número de parcelas aumente de forma consistente o efeito do atraso sobre a avaliação.
# MAGIC
# MAGIC Nos pedidos entregues no prazo, o maior percentual de avaliações negativas apareceu nas compras de maior valor. Já a forma de pagamento apresentou pouca diferença, principalmente entre boleto e cartão. Isso responde à terceira pergunta do MVP sem atribuir ao pagamento uma influência que os dados não demonstraram.
# MAGIC
# MAGIC Esta é uma análise descritiva e não permite afirmar causalidade. Além disso, 4.651 avaliações selecionadas foram respondidas antes do registro da entrega. Nesses casos, o resultado deve ser entendido como uma avaliação da experiência da compra até aquele momento, e não necessariamente do produto já recebido. Também foram sinalizados os grupos com menos de 30 pedidos, evitando conclusões fortes baseadas em poucos casos.
# MAGIC
# MAGIC No conjunto dos resultados, o pipeline permitiu organizar dados de pedidos, itens, pagamentos e avaliações em uma visão única por pedido. A principal conclusão é que o atraso está associado à piora da avaliação, enquanto valor, forma de pagamento e parcelamento ajudam a detalhar alguns grupos, mas não apresentam um efeito estável em todas as faixas.
# MAGIC
