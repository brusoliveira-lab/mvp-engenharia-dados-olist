# Catálogo de dados — MVP Olist

Este catálogo descreve as tabelas criadas pelo pipeline. O padrão de nomes usa
`snake_case`, datas como `timestamp`, valores monetários como `decimal(18,2)` e
indicadores booleanos iniciados por `flag_`.

## Origem e licença

- Fonte: Olist Brazilian E-Commerce Public Dataset, no Kaggle.
- Licença informada na página da base: CC BY-NC-SA 4.0.
- Uso adotado: somente neste MVP acadêmico, sem finalidade comercial.
- Os CSVs originais não serão publicados no GitHub.
- Arquivos usados: pedidos, itens, pagamentos e avaliações.

## Regras principais

- A unidade da análise é o pedido (`order_id`).
- A nota 1 ou 2 é negativa, 3 é neutra e 4 ou 5 é positiva.
- Atraso é a diferença, em dias, entre a entrega ao cliente e a data estimada.
- Só entram no cálculo de atraso pedidos entregues, com datas válidas e sem cronologia impossível.
- Quando há mais de uma avaliação válida, é usada a resposta mais recente. A primeira nota é preservada para controle.
- Itens e pagamentos são agregados antes da junção com pedidos.
- Pagamento combinado significa que o mesmo pedido tem mais de um tipo de pagamento.
- O recorte de parcelamento considera somente pedidos pagos exclusivamente com cartão de crédito.

## Domínios e expectativas de qualidade

| Campo ou grupo | Domínio esperado | Tratamento ou uso |
| --- | --- | --- |
| `order_id` | Texto não vazio; único em pedidos e na Gold | Chave de integração e teste de cardinalidade |
| `order_status` | Categorias informadas pela Olist; `delivered` identifica entrega concluída | Outros status não entram no cálculo de atraso |
| Datas do pedido | Timestamp válido | Datas ausentes ou cronologia impossível são sinalizadas |
| `valor_produto` e `valor_frete` | Decimal maior ou igual a zero | Conversão inválida ou valor negativo marca item inválido |
| `valor_pagamento` | Decimal maior que zero | Valor ausente, inválido ou não positivo marca pagamento inválido |
| `parcelas` | Número inteiro; no cartão, valores positivos formam os grupos analíticos | Zero ou valor inconsistente recebe regra de exceção |
| `nota_avaliacao` | Número inteiro de 1 a 5 | Fora desse intervalo, a avaliação não é válida |
| `classe_avaliacao` | `negativa`, `neutra` ou `positiva` | Derivada da nota 1–2, 3 ou 4–5 |
| `faixa_atraso` | `no_prazo`, `1_a_3_dias`, `4_a_7_dias`, `8_a_14_dias`, `15_dias_ou_mais` ou `nao_elegivel` | Derivada da diferença entre entrega real e estimada |
| `faixa_valor_pedido` | `ate_50`, `50_01_a_100`, `100_01_a_200`, `acima_de_200` ou `sem_valor` | Derivada da soma dos produtos, sem frete |

As faixas acima são regras do MVP, e não limites universais da base. Campos de
texto livre, como os comentários das avaliações, não possuem lista fechada de
valores e não participam das perguntas principais.

Para os demais campos, usei estas expectativas gerais:

| Grupo de campos | Domínio esperado |
| --- | --- |
| Identificadores | Texto não vazio; a unicidade é exigida apenas quando o campo representa a chave da tabela |
| Campos de auditoria | Data e hora válida, nome de arquivo ou identificador de lote |
| Indicadores `flag_` | `true` ou `false` |
| Quantidades | Número inteiro maior ou igual a zero |
| Percentuais | Valor entre 0 e 100 |
| Valores monetários | Decimal maior ou igual a zero; a diferença entre pagamento e itens pode ser positiva, negativa ou zero |
| Datas tratadas | `timestamp` válido ou nulo quando a informação não existe na origem |
| Comentários | Texto livre, podendo estar vazio porque não participa das perguntas do MVP |

## Camada Bronze

As quatro tabelas Bronze mantêm as colunas originais como texto e acrescentam
três campos de auditoria.

### Campos de auditoria comuns

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `_arquivo_origem` | string | Nome do CSV carregado. |
| `_data_ingestao` | timestamp | Horário em que a carga foi executada. |
| `_lote_ingestao` | string | Identificador UTC da execução. |

### `bronze_pedidos`

| Campo original | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `customer_id` | string | Identificador do cliente naquela compra. |
| `order_status` | string | Situação do pedido. |
| `order_purchase_timestamp` | string | Data e hora da compra, ainda sem conversão. |
| `order_approved_at` | string | Data e hora da aprovação, ainda sem conversão. |
| `order_delivered_carrier_date` | string | Data de entrega à transportadora, ainda sem conversão. |
| `order_delivered_customer_date` | string | Data de entrega ao cliente, ainda sem conversão. |
| `order_estimated_delivery_date` | string | Data estimada de entrega, ainda sem conversão. |

### `bronze_itens`

| Campo original | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `order_item_id` | string | Número sequencial do item no pedido, ainda sem conversão. |
| `product_id` | string | Identificador do produto. |
| `seller_id` | string | Identificador do vendedor. |
| `shipping_limit_date` | string | Data limite de envio, ainda sem conversão. |
| `price` | string | Preço do produto, ainda sem conversão. |
| `freight_value` | string | Valor do frete do item, ainda sem conversão. |

### `bronze_pagamentos`

| Campo original | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `payment_sequential` | string | Sequência do registro de pagamento, ainda sem conversão. |
| `payment_type` | string | Tipo de pagamento. |
| `payment_installments` | string | Número de parcelas, ainda sem conversão. |
| `payment_value` | string | Valor registrado no pagamento, ainda sem conversão. |

### `bronze_avaliacoes`

| Campo original | Tipo | Descrição |
| --- | --- | --- |
| `review_id` | string | Identificador da avaliação. |
| `order_id` | string | Identificador do pedido. |
| `review_score` | string | Nota de 1 a 5, ainda sem conversão. |
| `review_comment_title` | string | Título do comentário. |
| `review_comment_message` | string | Texto do comentário. |
| `review_creation_date` | string | Data de criação da avaliação, ainda sem conversão. |
| `review_answer_timestamp` | string | Data e hora da resposta, ainda sem conversão. |

## Camada Silver

### `silver_pedidos`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `customer_id` | string | Identificador do cliente naquela compra. |
| `order_status` | string | Situação do pedido. |
| `data_compra` | timestamp | Data e hora da compra. |
| `data_aprovacao` | timestamp | Data e hora da aprovação. |
| `data_entrega_transportadora` | timestamp | Data de entrega à transportadora. |
| `data_entrega_cliente` | timestamp | Data de entrega ao cliente. |
| `data_entrega_estimada` | timestamp | Data estimada de entrega. |
| `flag_status_entregue` | boolean | Indica status `delivered`. |
| `flag_data_entrega_ausente` | boolean | Indica falta da data real de entrega. |
| `flag_data_estimada_ausente` | boolean | Indica falta da data estimada. |
| `flag_cronologia_invalida` | boolean | Indica entrega anterior à compra ou à entrega à transportadora. |
| `flag_elegivel_atraso` | boolean | Indica pedido apto ao cálculo de atraso. |
| `_arquivo_origem` | string | Arquivo de origem. |
| `_data_ingestao` | timestamp | Horário da carga. |
| `_lote_ingestao` | string | Identificador da carga. |

### `silver_itens`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `numero_item` | integer | Número sequencial do item. |
| `product_id` | string | Identificador do produto. |
| `seller_id` | string | Identificador do vendedor. |
| `data_limite_envio` | timestamp | Data limite de envio. |
| `valor_produto` | decimal(18,2) | Preço do produto. |
| `valor_frete` | decimal(18,2) | Frete do item. |
| `flag_item_invalido` | boolean | Marca falha de conversão ou valor negativo. |

### `silver_itens_por_pedido`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `quantidade_itens` | long | Número de registros de item. |
| `valor_produtos` | decimal(18,2) | Soma dos produtos. |
| `valor_frete` | decimal(18,2) | Soma dos fretes. |
| `tem_item_invalido` | integer | Maior indicador de item inválido no pedido. |
| `valor_compra` | decimal(18,2) | Soma dos produtos, sem frete; base das faixas de valor. |
| `valor_total_itens_frete` | decimal(18,2) | Soma dos produtos e dos fretes, usada na reconciliação com pagamentos. |
| `flag_item_valido` | boolean | Indica que todos os itens passaram pelas regras. |

### `silver_pagamentos`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `sequencia_pagamento` | integer | Sequência do pagamento. |
| `tipo_pagamento` | string | Cartão, boleto, voucher, débito ou outro valor da origem. |
| `parcelas` | integer | Quantidade de parcelas. |
| `valor_pagamento` | decimal(18,2) | Valor do registro. |
| `flag_pagamento_invalido` | boolean | Marca campo obrigatório inválido ou valor não positivo. |

### `silver_pagamentos_por_pedido`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `quantidade_pagamentos` | long | Número de registros de pagamento. |
| `valor_total_pagamento` | decimal(18,2) | Soma dos pagamentos. |
| `tipos_pagamento` | array<string> | Tipos distintos, em ordem alfabética. |
| `tem_pagamento_invalido` | integer | Maior indicador de pagamento inválido. |
| `quantidade_planos_cartao` | long | Quantidade de números de parcelas distintos no cartão. |
| `maior_numero_parcelas_cartao` | integer | Maior quantidade de parcelas em cartão. |
| `quantidade_tipos_pagamento` | integer | Quantidade de tipos usados no pedido. |
| `combinacao_pagamento` | string | Combinação exata dos tipos usados, em ordem alfabética. |
| `modalidade_pagamento` | string | Tipo único, `combinado` ou `sem_pagamento`. |
| `flag_pagamento_valido` | boolean | Indica que todos os pagamentos passaram pelas regras. |
| `grupo_parcelamento_cartao` | string | `1_parcela`, `2_ou_mais`, regra de exceção ou `nao_aplicavel`. |

### `silver_avaliacoes`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `review_id` | string | Identificador da avaliação. |
| `order_id` | string | Identificador do pedido. |
| `nota_avaliacao` | integer | Nota convertida para número inteiro. |
| `review_comment_title` | string | Título do comentário. |
| `review_comment_message` | string | Texto do comentário. |
| `data_criacao_avaliacao` | timestamp | Data de criação. |
| `data_resposta_avaliacao` | timestamp | Data e hora da resposta. |
| `flag_avaliacao_valida` | boolean | Indica nota entre 1 e 5. |

### `silver_avaliacoes_por_pedido`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `order_id` | string | Identificador do pedido. |
| `review_id` | string | Avaliação mais recente escolhida. |
| `nota_avaliacao` | integer | Última nota válida. |
| `review_comment_title` | string | Título associado à nota escolhida. |
| `review_comment_message` | string | Comentário associado à nota escolhida. |
| `data_criacao_avaliacao` | timestamp | Criação da avaliação escolhida. |
| `data_resposta_avaliacao` | timestamp | Resposta da avaliação escolhida. |
| `primeira_nota_avaliacao` | integer | Primeira nota válida do pedido. |
| `data_primeira_avaliacao` | timestamp | Data da primeira resposta válida. |
| `quantidade_avaliacoes_validas` | long | Número de avaliações válidas do pedido. |
| `classe_avaliacao` | string | `negativa`, `neutra` ou `positiva`. |
| `flag_nota_alterada` | boolean | Indica que a última nota válida é diferente da primeira. |

## Camada Gold

### `gold_pedidos_analitico`

Esta tabela reúne todos os campos de `silver_pedidos`,
`silver_itens_por_pedido`, `silver_pagamentos_por_pedido` e
`silver_avaliacoes_por_pedido`, além dos campos calculados abaixo. A chave deve
continuar sendo `order_id`, com uma linha por pedido.

#### Campos recebidos das tabelas Silver

| Campo | Tipo | Origem e uso na Gold |
| --- | --- | --- |
| `order_id` | string | Chave do pedido, recebida de `silver_pedidos`. |
| `customer_id` | string | Cliente associado ao pedido. |
| `order_status` | string | Situação do pedido. |
| `data_compra` | timestamp | Data e hora da compra. |
| `data_aprovacao` | timestamp | Data e hora da aprovação. |
| `data_entrega_transportadora` | timestamp | Data de entrega à transportadora. |
| `data_entrega_cliente` | timestamp | Data de entrega ao cliente. |
| `data_entrega_estimada` | timestamp | Data prevista para a entrega. |
| `flag_status_entregue` | boolean | Indica se o status é `delivered`. |
| `flag_data_entrega_ausente` | boolean | Indica falta da data real de entrega. |
| `flag_data_estimada_ausente` | boolean | Indica falta da data prevista. |
| `flag_cronologia_invalida` | boolean | Indica uma sequência de datas impossível. |
| `flag_elegivel_atraso` | boolean | Indica se o pedido pode entrar no cálculo de atraso. |
| `_arquivo_origem` | string | Arquivo de origem do pedido. |
| `_data_ingestao` | timestamp | Horário da carga. |
| `_lote_ingestao` | string | Identificador da carga. |
| `quantidade_itens` | long | Quantidade de registros de item no pedido. |
| `valor_produtos` | decimal(18,2) | Soma dos preços dos produtos. |
| `valor_frete` | decimal(18,2) | Soma dos fretes. |
| `tem_item_invalido` | integer | Indica se algum item falhou nas regras. |
| `valor_compra` | decimal(18,2) | Soma dos produtos, sem frete. |
| `valor_total_itens_frete` | decimal(18,2) | Soma dos produtos e fretes. |
| `flag_item_valido` | boolean | Indica que todos os itens passaram pelas regras. |
| `quantidade_pagamentos` | long | Quantidade de registros de pagamento. |
| `valor_total_pagamento` | decimal(18,2) | Soma dos pagamentos do pedido. |
| `tipos_pagamento` | array<string> | Tipos de pagamento usados no pedido. |
| `tem_pagamento_invalido` | integer | Indica se algum pagamento falhou nas regras. |
| `quantidade_planos_cartao` | long | Quantidade de números de parcelas distintos no cartão. |
| `maior_numero_parcelas_cartao` | integer | Maior número de parcelas registrado no cartão. |
| `quantidade_tipos_pagamento` | integer | Quantidade de modalidades usadas no pedido. |
| `combinacao_pagamento` | string | Combinação das modalidades usadas. |
| `modalidade_pagamento` | string | Modalidade única, `combinado` ou `sem_pagamento`. |
| `flag_pagamento_valido` | boolean | Indica que todos os pagamentos passaram pelas regras. |
| `grupo_parcelamento_cartao` | string | Grupo de parcelas usado na análise. |
| `review_id` | string | Identificador da avaliação escolhida. |
| `nota_avaliacao` | integer | Última nota válida do pedido. |
| `review_comment_title` | string | Título da avaliação escolhida. |
| `review_comment_message` | string | Comentário da avaliação escolhida. |
| `data_criacao_avaliacao` | timestamp | Data de criação da avaliação escolhida. |
| `data_resposta_avaliacao` | timestamp | Data da resposta escolhida. |
| `primeira_nota_avaliacao` | integer | Primeira nota válida do pedido. |
| `data_primeira_avaliacao` | timestamp | Data da primeira resposta válida. |
| `quantidade_avaliacoes_validas` | long | Quantidade de avaliações válidas. |
| `classe_avaliacao` | string | `negativa`, `neutra` ou `positiva`. |
| `flag_nota_alterada` | boolean | Indica mudança entre a primeira e a última nota. |

#### Campos calculados na Gold

| Campo calculado | Tipo | Descrição |
| --- | --- | --- |
| `dias_atraso` | integer | Dias entre entrega real e estimada; nulo quando não elegível. |
| `faixa_atraso` | string | `no_prazo`, `1_a_3_dias`, `4_a_7_dias`, `8_a_14_dias`, `15_dias_ou_mais` ou `nao_elegivel`. |
| `ordem_faixa_atraso` | integer | Ordem numérica usada para exibir as faixas de atraso. |
| `flag_atrasado` | boolean | Indica atraso maior que zero dia. |
| `faixa_valor_pedido` | string | `ate_50`, `50_01_a_100`, `100_01_a_200`, `acima_de_200` ou `sem_valor`. |
| `ordem_faixa_valor` | integer | Ordem numérica usada para exibir as faixas de valor. |
| `diferenca_valor_pagamento_item` | decimal(18,2) | Pagamento total menos produtos e frete. |
| `flag_divergencia_valores` | boolean | Diferença absoluta de valor acima de R$ 0,01. |
| `flag_sem_itens` | boolean | Pedido sem correspondência em itens. |
| `flag_sem_pagamento` | boolean | Pedido sem correspondência em pagamentos. |
| `flag_sem_avaliacao` | boolean | Pedido sem avaliação válida. |
| `flag_avaliacao_antes_entrega` | boolean | Resposta registrada antes da entrega ao cliente. |
| `momento_resposta_avaliacao` | string | Antes da entrega, na entrega ou depois, ou sem data de resposta. |
| `flag_elegivel_analise_principal` | boolean | Pedido elegível ao atraso e com avaliação válida. |

### Tabelas de resposta

| Tabela | Dimensões | Filtro adicional |
| --- | --- | --- |
| `gold_avaliacao_por_atraso` | `faixa_atraso` | Nenhum além da elegibilidade principal. |
| `gold_avaliacao_por_atraso_valor` | `faixa_atraso`, `faixa_valor_pedido` | Itens válidos. |
| `gold_avaliacao_por_atraso_pagamento` | `faixa_atraso`, `modalidade_pagamento` | Pagamentos válidos. |
| `gold_avaliacao_cartao_parcelas` | `faixa_atraso`, `grupo_parcelamento_cartao` | Pagamento válido e exclusivamente em cartão. |
| `gold_avaliacao_por_atraso_momento` | `faixa_atraso`, `momento_resposta_avaliacao` | Recorte complementar sobre quando a avaliação foi respondida. |

Tipos e significado das dimensões usadas nessas tabelas:

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `ordem_faixa_atraso` | integer | Ordem usada para exibir as faixas de atraso. |
| `faixa_atraso` | string | Grupo de atraso do pedido. |
| `ordem_faixa_valor` | integer | Ordem usada para exibir as faixas de valor. |
| `faixa_valor_pedido` | string | Grupo de valor da compra, sem frete. |
| `modalidade_pagamento` | string | Modalidade única, combinação ou ausência de pagamento. |
| `grupo_parcelamento_cartao` | string | `1_parcela` ou `2_ou_mais` no recorte de cartão. |
| `momento_resposta_avaliacao` | string | `antes_da_entrega`, `na_entrega_ou_depois` ou `sem_data_resposta`. |

Todas as tabelas de resposta possuem estes indicadores:

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `quantidade_pedidos` | long | Total de pedidos usado no cálculo dos percentuais do grupo. |
| `nota_media` | double | Média da nota selecionada. |
| `avaliacoes_negativas` | long | Quantidade de notas 1 ou 2. |
| `avaliacoes_neutras` | long | Quantidade de notas 3. |
| `avaliacoes_positivas` | long | Quantidade de notas 4 ou 5. |
| `percentual_negativas` | double | Percentual de avaliações negativas. |
| `percentual_neutras` | double | Percentual de avaliações neutras. |
| `percentual_positivas` | double | Percentual de avaliações positivas. |
| `flag_grupo_pequeno` | boolean | Alerta operacional quando o grupo tem menos de 30 pedidos. |

### `gold_controle_qualidade`

| Campo | Tipo | Descrição |
| --- | --- | --- |
| `controle` | string | Nome do teste ou da contagem. |
| `quantidade` | long | Resultado observado. |
| `status` | string | `OK`, `ATENCAO`, `ERRO` ou `INFORMATIVO`. |
| `criterio` | string | Motivo do controle. |
| `data_execucao` | timestamp | Horário da execução. |
