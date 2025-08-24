-- =====================================================
-- 1. CRIAR SCHEMA
-- =====================================================
CREATE SCHEMA IF NOT EXISTS dw_mcmv;

-- =====================================================
-- 2. TABELA STAGING
-- =====================================================
DROP TABLE IF EXISTS dw_mcmv.stg_mcmv;

CREATE TABLE dw_mcmv.stg_mcmv (
    data_referencia DATE,
    cod_ibge TEXT,
    txt_regiao TEXT,
    txt_sigla_uf CHAR(2),
    txt_regiao_1 TEXT,
    dt_assinatura TEXT,
    cod_operacao TEXT,
    txt_nome_empreendimento TEXT,
    txt_nome_agente_financeiro TEXT,
    txt_modalidade TEXT,
    txt_situacao_empreendimento TEXT,
    qtd_uh NUMERIC,
    qtd_uh_entregues NUMERIC,
    qtd_uh_vigentes NUMERIC,
    qtd_uh_distratadas NUMERIC,
    val_contratado_total NUMERIC(15,2),
    val_desembolsado NUMERIC(15,2),
    txt_cnpj_construtora_entidade TEXT,
    txt_nome_construtora_entidade TEXT,
    txt_endereco TEXT,
    txt_cep TEXT
);


-- =====================================================
-- 3. DIMENSÕES
-- =====================================================
DROP TABLE IF EXISTS dw_mcmv.dim_tempo CASCADE;
CREATE TABLE dw_mcmv.dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    data DATE UNIQUE,
    ano INT,
    mes INT,
    dia INT
);

DROP TABLE IF EXISTS dw_mcmv.dim_localidade CASCADE;
CREATE TABLE dw_mcmv.dim_localidade (
    id_localidade SERIAL PRIMARY KEY,
    municipio TEXT,
    uf CHAR(2),
    regiao TEXT,
    UNIQUE(municipio, uf, regiao)
);

DROP TABLE IF EXISTS dw_mcmv.dim_empreendimento CASCADE;
CREATE TABLE dw_mcmv.dim_empreendimento (
    id_empreendimento SERIAL PRIMARY KEY,
    nome TEXT,
    modalidade TEXT,
    situacao TEXT,
    UNIQUE(nome, modalidade, situacao)
);

-- =====================================================
-- 4. TABELA FATO
-- =====================================================
DROP TABLE IF EXISTS dw_mcmv.fato_empreendimento;
CREATE TABLE dw_mcmv.fato_empreendimento (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INT REFERENCES dw_mcmv.dim_tempo(id_tempo),
    id_localidade INT REFERENCES dw_mcmv.dim_localidade(id_localidade),
    id_empreendimento INT REFERENCES dw_mcmv.dim_empreendimento(id_empreendimento),
    qtd_uh INT,
    qtd_uh_entregues INT,
    qtd_uh_vigentes INT,
    qtd_uh_distratadas INT,
    val_contratado_total NUMERIC(15,2)
);

-- =====================================================
-- 5. CARREGAR CSV PARA A STAGING
-- =====================================================
COPY dw_mcmv.stg_mcmv(
    data_referencia,
    cod_ibge,
    txt_regiao,
    txt_sigla_uf,
    txt_regiao_1,
    dt_assinatura,
    cod_operacao,
    txt_nome_empreendimento,
    txt_nome_agente_financeiro,
    txt_modalidade,
    txt_situacao_empreendimento,
    qtd_uh,
    qtd_uh_entregues,
    qtd_uh_vigentes,
    qtd_uh_distratadas,
    val_contratado_total,
    val_desembolsado,
    txt_cnpj_construtora_entidade,
    txt_nome_construtora_entidade,
    txt_endereco,
    txt_cep
)
FROM '/data/dados_tratados_utf8.csv'
DELIMITER ';'
CSV HEADER
ENCODING 'UTF8';


-- =====================================================
-- 6. POPULAR DIMENSÕES
-- =====================================================
-- Dimensão Tempo (ajuste automático de formato de data)
INSERT INTO dw_mcmv.dim_tempo (data, ano, mes, dia)
SELECT DISTINCT
    -- tenta converter para DD/MM/YYYY, se falhar assume YYYY-MM-DD
    CASE
        WHEN dt_assinatura ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(dt_assinatura, 'DD/MM/YYYY')
        WHEN dt_assinatura ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(dt_assinatura, 'YYYY-MM-DD')
        ELSE NULL
    END AS data,
    EXTRACT(YEAR FROM (
        CASE
            WHEN dt_assinatura ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(dt_assinatura, 'DD/MM/YYYY')
            WHEN dt_assinatura ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(dt_assinatura, 'YYYY-MM-DD')
            ELSE NULL
        END
    ))::INT,
    EXTRACT(MONTH FROM (
        CASE
            WHEN dt_assinatura ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(dt_assinatura, 'DD/MM/YYYY')
            WHEN dt_assinatura ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(dt_assinatura, 'YYYY-MM-DD')
            ELSE NULL
        END
    ))::INT,
    EXTRACT(DAY FROM (
        CASE
            WHEN dt_assinatura ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(dt_assinatura, 'DD/MM/YYYY')
            WHEN dt_assinatura ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(dt_assinatura, 'YYYY-MM-DD')
            ELSE NULL
        END
    ))::INT
FROM dw_mcmv.stg_mcmv
WHERE dt_assinatura IS NOT NULL
ON CONFLICT (data) DO NOTHING;

-- Dimensão Localidade
INSERT INTO dw_mcmv.dim_localidade (municipio, uf, regiao)
SELECT DISTINCT
    TRIM(txt_nome_municipio),
    TRIM(txt_sigla_uf),
    TRIM(txt_regiao_2)
FROM dw_mcmv.stg_mcmv
WHERE txt_nome_municipio IS NOT NULL
ON CONFLICT (municipio, uf, regiao) DO NOTHING;

-- Dimensão Empreendimento
INSERT INTO dw_mcmv.dim_empreendimento (nome, modalidade, situacao)
SELECT DISTINCT
    TRIM(txt_nome_empreendimento),
    TRIM(txt_modalidade),
    TRIM(txt_situacao_empreendimento)
FROM dw_mcmv.stg_mcmv
WHERE txt_nome_empreendimento IS NOT NULL
ON CONFLICT (nome, modalidade, situacao) DO NOTHING;

-- =====================================================
-- 7. POPULAR TABELA FATO
-- =====================================================
INSERT INTO dw_mcmv.fato_empreendimento (
    id_tempo, id_localidade, id_empreendimento,
    qtd_uh, qtd_uh_entregues, qtd_uh_vigentes, qtd_uh_distratadas,
    val_contratado_total
)
SELECT 
    t.id_tempo,
    l.id_localidade,
    e.id_empreendimento,
    s.qtd_uh,
    s.qtd_uh_entregues,
    s.qtd_uh_vigentes,
    s.qtd_uh_distratadas,
    s.val_contratado_total
FROM dw_mcmv.stg_mcmv s
LEFT JOIN dw_mcmv.dim_tempo t 
       ON t.data = (
            CASE
                WHEN s.dt_assinatura ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(s.dt_assinatura, 'DD/MM/YYYY')
                WHEN s.dt_assinatura ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(s.dt_assinatura, 'YYYY-MM-DD')
                ELSE NULL
            END
       )
LEFT JOIN dw_mcmv.dim_localidade l 
       ON l.municipio = TRIM(s.txt_nome_municipio)
      AND l.uf = TRIM(s.txt_sigla_uf)
      AND l.regiao = TRIM(s.txt_regiao_2)
LEFT JOIN dw_mcmv.dim_empreendimento e 
       ON e.nome = TRIM(s.txt_nome_empreendimento)
      AND e.modalidade = TRIM(s.txt_modalidade)
      AND e.situacao = TRIM(s.txt_situacao_empreendimento);
