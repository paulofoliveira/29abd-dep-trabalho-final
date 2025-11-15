# FIAP – MBA em Engenharia de Dados  
## Trabalho de Data Engineering – Pyspark  
**Professor:** Marcelo Barbosa  

---

## 🎯 Objetivo

Construir um projeto de **Data Engineering com PySpark** aplicando os conceitos estudados ao longo da disciplina.  
O foco é entregar um **pipeline de dados** capaz de gerar um relatório solicitado pela alta gestão da empresa.

---

## 🧩 Escopo de Negócio

A diretoria deseja analisar pedidos de venda **com pagamento recusado** (`status = false`), mas que, na **avaliação de fraude**, foram classificados como **legítimos** (`fraude = false`).

Com base nesses critérios, o trabalho deve:

- Selecionar somente os pedidos do **ano de 2025**
- Aplicar filtros de pagamento e fraude
- Selecionar os atributos específicos do relatório
- Ordenar corretamente os resultados
- Persistir a saída em **formato Parquet**

---

## 📊 Requisitos do Relatório

O conjunto final deve conter:

1. **ID do pedido**  
2. **Estado (UF)** onde o pedido foi feito  
3. **Forma de pagamento**  
4. **Valor total do pedido**  
5. **Data do pedido**

**Regras adicionais:**

- Apenas pedidos do ano **2025**
- Ordenação por:
  1. Estado (UF)
  2. Forma de pagamento
  3. Data do pedido
- Salvar o arquivo final em **Parquet**


