

-- Script para criar a tabela AS_CABECALHO_NOTA no Oracle
-- Tabela responsável por armazenar os cabeçalhos de nota sincronizados de cada empresa

-- Criar a tabela AS_CABECALHO_NOTA
CREATE TABLE AS_CABECALHO_NOTA (
    ID_SISTEMA NUMBER NOT NULL,
    NUNOTA NUMBER NOT NULL,
    CODTIPOPER NUMBER NOT NULL,
    CODTIPVENDA NUMBER,
    CODPARC NUMBER NOT NULL,
    CODVEND NUMBER,
    VLRNOTA NUMBER(15,2),
    DTNEG DATE,
    TIPMOV VARCHAR2(10) NOT NULL,
    SANKHYA_ATUAL CHAR(1) DEFAULT 'S' CHECK (SANKHYA_ATUAL IN ('S', 'N')),
    DT_ULT_CARGA TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    DT_CRIACAO TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_as_cabecalho_nota PRIMARY KEY (ID_SISTEMA, NUNOTA),
    CONSTRAINT fk_as_cabecalho_nota_empresa FOREIGN KEY (ID_SISTEMA) REFERENCES AD_CONTRATOS(ID_EMPRESA) ON DELETE CASCADE
);

-- Criar índices para melhorar performance
CREATE INDEX idx_as_cabecalho_nota_sistema ON AS_CABECALHO_NOTA(ID_SISTEMA);
CREATE INDEX idx_as_cabecalho_nota_atual ON AS_CABECALHO_NOTA(SANKHYA_ATUAL);
CREATE INDEX idx_as_cabecalho_nota_nunota ON AS_CABECALHO_NOTA(NUNOTA);
CREATE INDEX idx_as_cabecalho_nota_codparc ON AS_CABECALHO_NOTA(CODPARC);
CREATE INDEX idx_as_cabecalho_nota_codvend ON AS_CABECALHO_NOTA(CODVEND);
CREATE INDEX idx_as_cabecalho_nota_dtneg ON AS_CABECALHO_NOTA(DTNEG);
CREATE INDEX idx_as_cabecalho_nota_carga ON AS_CABECALHO_NOTA(DT_ULT_CARGA);

-- Criar trigger para atualizar DT_ULT_CARGA automaticamente
CREATE OR REPLACE TRIGGER trg_cabecalho_nota_atualizacao
BEFORE UPDATE ON AS_CABECALHO_NOTA
FOR EACH ROW
BEGIN
    :NEW.DT_ULT_CARGA := CURRENT_TIMESTAMP;
END;
/

-- Comentários nas colunas
COMMENT ON TABLE AS_CABECALHO_NOTA IS 'Tabela de sincronização de cabeçalhos de nota do Sankhya por empresa';
COMMENT ON COLUMN AS_CABECALHO_NOTA.ID_SISTEMA IS 'Identificador da empresa (segregador multi-tenant)';
COMMENT ON COLUMN AS_CABECALHO_NOTA.NUNOTA IS 'Número único da nota';
COMMENT ON COLUMN AS_CABECALHO_NOTA.CODTIPOPER IS 'Código do tipo de operação';
COMMENT ON COLUMN AS_CABECALHO_NOTA.CODTIPVENDA IS 'Código do tipo de venda/negociação';
COMMENT ON COLUMN AS_CABECALHO_NOTA.CODPARC IS 'Código do parceiro (cliente)';
COMMENT ON COLUMN AS_CABECALHO_NOTA.CODVEND IS 'Código do vendedor';
COMMENT ON COLUMN AS_CABECALHO_NOTA.VLRNOTA IS 'Valor total da nota';
COMMENT ON COLUMN AS_CABECALHO_NOTA.DTNEG IS 'Data de negociação';
COMMENT ON COLUMN AS_CABECALHO_NOTA.TIPMOV IS 'Tipo de movimento (P=Pedido, V=Venda, etc)';
COMMENT ON COLUMN AS_CABECALHO_NOTA.SANKHYA_ATUAL IS 'Indica se o registro está ativo no Sankhya (S=Sim, N=Soft Delete)';
COMMENT ON COLUMN AS_CABECALHO_NOTA.DT_ULT_CARGA IS 'Data da última sincronização deste registro';
COMMENT ON COLUMN AS_CABECALHO_NOTA.DT_CRIACAO IS 'Data de criação do registro na tabela';

-- Criar view para consultar apenas cabeçalhos de nota ativos
CREATE OR REPLACE VIEW VW_CABECALHO_NOTA_ATIVOS AS
SELECT 
    cn.*,
    c.EMPRESA,
    c.CNPJ
FROM AS_CABECALHO_NOTA cn
INNER JOIN AD_CONTRATOS c ON cn.ID_SISTEMA = c.ID_EMPRESA
WHERE cn.SANKHYA_ATUAL = 'S'
  AND c.ATIVO = 'S';

COMMENT ON VIEW VW_CABECALHO_NOTA_ATIVOS IS 'View de cabeçalhos de nota ativos sincronizados';

COMMIT;
