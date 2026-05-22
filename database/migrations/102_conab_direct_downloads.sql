-- Migration 102: CONAB direct-download bronze tables
-- Source: https://portaldeinformacoes.conab.gov.br/downloads/arquivos/
-- See reference_conab_direct_downloads.md for endpoint inventory.

-- 1. Inland freight rates by origin/destination/month
CREATE TABLE IF NOT EXISTS bronze.conab_freight (
    id              BIGSERIAL PRIMARY KEY,
    dsc_fonte       TEXT,               -- PESQUISA = survey, CONTRATO = contract
    municipio_origem TEXT,
    cod_ibge_origem TEXT,
    uf_origem       TEXT,
    municipio_destino TEXT,
    cod_ibge_destino TEXT,
    uf_destino      TEXT,
    ano             INTEGER,
    mes             INTEGER,
    distancia_km    NUMERIC,
    valor_frete_tonelada NUMERIC,        -- BRL per tonne
    valor_tonelada_km NUMERIC,           -- BRL per tonne-km
    collected_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_conab_freight_period ON bronze.conab_freight (ano, mes);
CREATE INDEX IF NOT EXISTS idx_conab_freight_route ON bronze.conab_freight (cod_ibge_origem, cod_ibge_destino);

-- 2. Minimum support prices by product/state/region (PGPM)
CREATE TABLE IF NOT EXISTS bronze.conab_min_prices (
    id              BIGSERIAL PRIMARY KEY,
    descricao_produto TEXT,
    id_produto      INTEGER,
    uf              TEXT,
    regionalizacao  TEXT,
    ano_inicio_vigencia INTEGER,
    mes_inicio_vigencia INTEGER,
    ano_termino_vigencia INTEGER,
    mes_termino_vigencia INTEGER,
    preco           NUMERIC,             -- BRL per dsc_unidade_comercializacao
    dsc_unidade_comercializacao TEXT,
    nome_normativo  TEXT,                -- Portaria/Decreto reference
    url             TEXT,
    collected_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_conab_min_prices_product ON bronze.conab_min_prices (id_produto, ano_inicio_vigencia);

-- 3. Production cost by enterprise/product/state/municipality
CREATE TABLE IF NOT EXISTS bronze.conab_production_cost (
    id              BIGSERIAL PRIMARY KEY,
    empreendimento  TEXT,                -- AGRICULTURA EMPRESARIAL etc.
    ano             INTEGER,
    mes             INTEGER,
    ano_mes         INTEGER,
    produto         TEXT,
    id_produto      INTEGER,
    safra           TEXT,
    uf              TEXT,
    municipio       TEXT,
    cod_ibge        TEXT,
    unidade_comercializacao TEXT,
    vlr_custo_variavel_ha NUMERIC,
    vlr_custo_variavel_unidade NUMERIC,
    vlr_custo_fixo_ha NUMERIC,
    vlr_custo_fixo_unidade NUMERIC,
    vlr_renda_fator_ha NUMERIC,
    vlr_renda_fator_unidade NUMERIC,
    collected_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_conab_prodcost_lookup ON bronze.conab_production_cost (id_produto, uf, ano);

-- 4. Supply & Demand by product/safra (CONAB OfertaDemanda)
--    Different shape from existing bronze.conab_supply_demand so new table.
CREATE TABLE IF NOT EXISTS bronze.conab_supply_demand_v2 (
    id              BIGSERIAL PRIMARY KEY,
    produto         TEXT,
    id_produto      INTEGER,
    dsc_safra       TEXT,                -- e.g., '2024/25'
    estoque_inicial_1000t NUMERIC,
    producao_1000t  NUMERIC,
    importacao_1000t NUMERIC,
    consumo_1000t   NUMERIC,
    exportacao_1000t NUMERIC,
    estoque_final_1000t NUMERIC,
    collected_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS conab_supply_demand_v2_unique
    ON bronze.conab_supply_demand_v2 (id_produto, dsc_safra);

-- 5. Stocks by product/municipality/month
CREATE TABLE IF NOT EXISTS bronze.conab_stocks (
    id              BIGSERIAL PRIMARY KEY,
    produto         TEXT,
    id_produto      INTEGER,
    nom_municipio   TEXT,
    cod_ibge        TEXT,
    uf              TEXT,
    num_ano         INTEGER,
    num_mes         INTEGER,
    conta_operacional TEXT,              -- ESTRATÉGICO, GOVERNO etc.
    qtd_estoque_kg  NUMERIC,
    collected_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_conab_stocks_lookup ON bronze.conab_stocks (id_produto, num_ano, num_mes);

-- 6. Registered warehouses (the Brazilian storage map — geocoded)
CREATE TABLE IF NOT EXISTS bronze.conab_warehouses (
    id              BIGSERIAL PRIMARY KEY,
    identificacao_armazem TEXT,           -- e.g., 35.0277.0001-4
    dsc_especie_armazem   TEXT,           -- e.g., CONVENCIONAL
    dsc_tipo_armazem      TEXT,           -- CONVENCIONAL, ESTRUTURAL etc.
    dsc_tipo_entidade     TEXT,           -- OFICIAL, COOPERATIVA, PARTICULAR
    dsc_tipo_pessoa       TEXT,           -- PESSOA JURÍDICA / FÍSICA
    nom_municipio         TEXT,
    cod_ibge              TEXT,
    uf                    TEXT,
    qtd_capacidade_estatica_t   NUMERIC,  -- Static / holding capacity, tonnes
    qtd_capacidade_expedicao_t  NUMERIC,  -- Shipping capacity, tonnes
    qtd_capacidade_recepcao_t   NUMERIC,  -- Receiving capacity, tonnes
    latitude              NUMERIC,
    longitude             NUMERIC,
    nome_armazenador      TEXT,
    endereco              TEXT,
    email                 TEXT,
    collected_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_conab_warehouses_geo ON bronze.conab_warehouses (uf, cod_ibge);
CREATE INDEX IF NOT EXISTS idx_conab_warehouses_id ON bronze.conab_warehouses (identificacao_armazem);

-- Optional: a convenience view for warehouse-by-state summary
CREATE OR REPLACE VIEW silver.conab_warehouse_state_summary AS
SELECT
    uf,
    COUNT(*) AS warehouse_count,
    SUM(qtd_capacidade_estatica_t) / 1000 AS total_static_capacity_kt,
    SUM(qtd_capacidade_expedicao_t) / 1000 AS total_ship_capacity_kt,
    SUM(qtd_capacidade_recepcao_t) / 1000 AS total_receive_capacity_kt,
    COUNT(DISTINCT nom_municipio) AS distinct_municipios
FROM bronze.conab_warehouses
WHERE uf IS NOT NULL
GROUP BY uf
ORDER BY total_static_capacity_kt DESC NULLS LAST;
