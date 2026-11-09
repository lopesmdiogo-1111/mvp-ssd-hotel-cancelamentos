# Catálogo de Dados — MVP Cancelamentos Hoteleiros

**Origem:** `hotel_bookings.csv` — *Hotel Booking Demand* (Kaggle, `jessemostipak/hotel-booking-demand`), dados de Antonio, Almeida e Nunes (2019), *Data in Brief* 22:41–49. Licença CC BY 4.0.
**Data da coleta:** 09/11/2026 (16,1 MB) · **Volume:** 119.390 linhas × 32 colunas · **Formato original:** CSV.
**Linhagem geral:** CSV → `bronze.hotel_bookings` (texto bruto + `_arquivo_origem`, `_data_ingestao`) → `silver.reservas` (tipado e tratado) / `silver.rejeitados` → `gold.*` (esquema estrela).


## gold.fato_reserva — grão: uma reserva

| Atributo | Tipo | Descrição | Domínio | Nulo? | Linhagem |
|---|---|---|---|---|---|
| id_reserva | bigint | Identificador sequencial criado no pipeline | único | Não | `monotonically_increasing_id()` na Silver |
| sk_hotel | int | Chave para `dim_hotel` | 1–2 | Não | junção com `dim_hotel` |
| sk_data_chegada | int | Chave para `dim_tempo` (AAAAMMDD) | 20150701–20170831 | Não | `arrival_date_year/month/day_of_month` |
| sk_canal | int | Chave para `dim_canal` | ≥ 1 | Não | junção com `dim_canal` |
| sk_perfil_cliente | int | Chave para `dim_perfil_cliente` | ≥ 1 | Não | junção com `dim_perfil_cliente` |
| flag_cancelada | boolean | Reserva cancelada | {true, false} | Não | `is_canceled = 1` |
| lead_time | int | Dias entre a reserva e a chegada | 0–737 | Não | `lead_time` |
| adr | decimal | Diária média (EUR) | 0–510 | Não | `adr`; negativos e > 1000 rejeitados |
| noites_total | int | Noites reservadas | 0–69 | Não | `stays_in_weekend_nights + stays_in_week_nights` |
| hospedes | int | Adultos + crianças + bebês | 1–55 | Não | `adults + children + babies`; zero rejeitado |
| receita_potencial | decimal | Receita se a reserva se concretizasse (EUR) | ≥ 0 | Não | `adr × noites_total` |
| alteracoes | int | Alterações feitas na reserva | 0–18 | Não | `booking_changes` |
| dias_cancel_chegada | int | Dias entre o cancelamento e a chegada | 0–526 | Sim: nulo se não cancelada | `data_chegada − reservation_status_date` |

## gold.dim_hotel
| Atributo | Tipo | Descrição | Domínio | Nulo? | Linhagem |
|---|---|---|---|---|---|
| sk_hotel | int | Chave substituta | 1–2 | Não | `row_number()` |
| tipo_hotel | string | Tipo do hotel | {City Hotel, Resort Hotel} | Não | `hotel` |

## gold.dim_tempo
| Atributo | Tipo | Descrição | Domínio | Nulo? | Linhagem |
|---|---|---|---|---|---|
| sk_data | int | Chave AAAAMMDD | 20150701–20170831 | Não | derivado de `data` |
| data | date | Data de chegada | 01/07/2015–31/08/2017 | Não | ano + mês (nome→número) + dia |
| ano, mes, semana | int | Componentes da data | 2015–2017; 1–12; 1–53 | Não | funções de data |
| temporada | string | Temporada (hemisfério norte) | {alta: jun–ago, baixa: dez–fev, media} | Não | regra sobre `mes` |

## gold.dim_canal
| Atributo | Tipo | Descrição | Domínio | Nulo? | Linhagem |
|---|---|---|---|---|---|
| sk_canal | int | Chave substituta | ≥ 1 | Não | `row_number()` |
| segmento | string | Segmento de mercado | {Aviation, Complementary, Corporate, Direct, Groups, Offline TA/TO, Online TA, Undefined} | Não | `market_segment` |
| canal_distrib | string | Canal de distribuição | {Corporate, Direct, GDS, TA/TO, Undefined} | Não | `distribution_channel` |
| tipo_deposito | string | Política de depósito | {No Deposit, Non Refund, Refundable} | Não | `deposit_type` |
| agente | string | Código do agente de viagem | códigos numéricos ou `sem_agente` | Não | `agent`; "NULL" → `sem_agente` |

## gold.dim_perfil_cliente
| Atributo | Tipo | Descrição | Domínio | Nulo? | Linhagem |
|---|---|---|---|---|---|
| sk_perfil | int | Chave substituta | ≥ 1 | Não | `row_number()` |
| pais | string | País de origem (ISO 3166-1 alfa-3) | códigos ISO ou `desconhecido` | Não | `country`; "NULL" → `desconhecido` |
| tipo_cliente | string | Tipo de reserva/cliente | {Contract, Group, Transient, Transient-Party} | Não | `customer_type` |
| recorrente | boolean | Hóspede recorrente | {true, false} | Não | `is_repeated_guest = 1` |
| faixa_cancel_prev | string | Cancelamentos anteriores do cliente | {0, 1, 2+} | Não | faixas de `previous_cancellations` |

## Atributos de origem não levados à Gold
`company` (majoritariamente nulo), `reserved_room_type`, `assigned_room_type`, `days_in_waiting_list`, `required_car_parking_spaces`, `total_of_special_requests`, `previous_bookings_not_canceled` e `meal` (mantido apenas na Silver como `refeicao`). `reservation_status` foi usado só para checar consistência com `is_canceled`, pois é registrado depois do desfecho.
