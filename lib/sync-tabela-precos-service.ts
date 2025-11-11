
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';

interface TabelaPreco {
  NUTAB: number;
  DTVIGOR?: string;
  PERCENTUAL?: number;
  UTILIZADECCUSTO?: string;
  CODTABORIG?: number;
  DTALTER?: string;
  CODTAB?: number;
  JAPE_ID?: string;
}

interface SyncResult {
  success: boolean;
  idSistema: number;
  empresa: string;
  totalRegistros: number;
  registrosInseridos: number;
  registrosAtualizados: number;
  registrosDeletados: number;
  dataInicio: string;
  dataFim: string;
  duracao: number;
  erro?: string;
}

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";

/**
 * Converter data do formato DD/MM/YYYY para YYYY-MM-DD
 */
function converterDataSankhya(dataSankhya?: string): string | null {
  if (!dataSankhya) return null;
  
  try {
    const [dia, mes, ano] = dataSankhya.split('/');
    if (!dia || !mes || !ano) return null;
    return `${ano}-${mes.padStart(2, '0')}-${dia.padStart(2, '0')}`;
  } catch {
    return null;
  }
}

/**
 * Buscar tabelas de preços do Sankhya
 */
async function buscarTabelaPrecosSankhya(idSistema: number, bearerToken: string): Promise<TabelaPreco[]> {
  console.log(`📋 [Sync] Buscando tabelas de preços do Sankhya para empresa ${idSistema}...`);

  const payload = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "TabelaPreco",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,
        "entity": {
          "fieldset": {
            "list": "NUTAB, DTVIGOR, PERCENTUAL, UTILIZADECCUSTO, CODTABORIG, DTALTER, CODTAB, JAPE_ID"
          }
        }
      }
    }
  };

  try {
    const response = await axios.post(URL_CONSULTA_SERVICO, payload, {
      headers: {
        'Authorization': `Bearer ${bearerToken}`,
        'Content-Type': 'application/json'
      },
      timeout: 30000
    });

    if (!response.data?.responseBody?.entities?.entity) {
      console.log('⚠️ [Sync] Nenhuma tabela de preços encontrada');
      return [];
    }

    const entities = response.data.responseBody.entities;
    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

    const tabelasPrecos = entityArray.map((rawEntity: any) => {
      const cleanObject: any = {};
      for (let i = 0; i < fieldNames.length; i++) {
        const fieldKey = `f${i}`;
        const fieldName = fieldNames[i];
        if (rawEntity[fieldKey]) {
          cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
      }
      return cleanObject as TabelaPreco;
    });

    console.log(`✅ [Sync] ${tabelasPrecos.length} tabelas de preços encontradas`);
    return tabelasPrecos;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao buscar tabelas de preços do Sankhya:', error.message);
    throw new Error(`Erro ao buscar tabelas de preços: ${error.message}`);
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_TABELA_PRECOS 
     SET SANKHYA_ATUAL = 'N', 
         DT_ULT_CARGA = CURRENT_TIMESTAMP 
     WHERE ID_SISTEMA = :idSistema 
       AND SANKHYA_ATUAL = 'S'`,
    [idSistema],
    { autoCommit: false }
  );

  const rowsAffected = result.rowsAffected || 0;
  console.log(`🗑️ [Sync] ${rowsAffected} registros marcados como não atuais`);
  return rowsAffected;
}

/**
 * Upsert (inserir ou atualizar) tabelas de preços
 */
async function upsertTabelasPrecos(
  connection: oracledb.Connection,
  idSistema: number,
  tabelasPrecos: TabelaPreco[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  for (const tabela of tabelasPrecos) {
    try {
      const checkResult = await connection.execute(
        `SELECT COUNT(*) as count FROM AS_TABELA_PRECOS 
         WHERE ID_SISTEMA = :idSistema AND NUTAB = :nutab`,
        [idSistema, tabela.NUTAB],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const exists = (checkResult.rows as any[])[0].COUNT > 0;

      if (exists) {
        await connection.execute(
          `UPDATE AS_TABELA_PRECOS SET
            DTVIGOR = TO_DATE(:dtvigor, 'YYYY-MM-DD'),
            PERCENTUAL = :percentual,
            UTILIZADECCUSTO = :utilizadeccusto,
            CODTABORIG = :codtaborig,
            DTALTER = TO_DATE(:dtalter, 'YYYY-MM-DD'),
            CODTAB = :codtab,
            JAPE_ID = :jape_id,
            SANKHYA_ATUAL = 'S',
            DT_ULT_CARGA = CURRENT_TIMESTAMP
          WHERE ID_SISTEMA = :idSistema AND NUTAB = :nutab`,
          {
            dtvigor: converterDataSankhya(tabela.DTVIGOR),
            percentual: tabela.PERCENTUAL || null,
            utilizadeccusto: tabela.UTILIZADECCUSTO || null,
            codtaborig: tabela.CODTABORIG || null,
            dtalter: converterDataSankhya(tabela.DTALTER),
            codtab: tabela.CODTAB || null,
            jape_id: tabela.JAPE_ID || null,
            idSistema,
            nutab: tabela.NUTAB
          },
          { autoCommit: false }
        );
        atualizados++;
      } else {
        await connection.execute(
          `INSERT INTO AS_TABELA_PRECOS (
            ID_SISTEMA, NUTAB, DTVIGOR, PERCENTUAL,
            UTILIZADECCUSTO, CODTABORIG, DTALTER, CODTAB, JAPE_ID,
            SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
          ) VALUES (
            :idSistema, :nutab, TO_DATE(:dtvigor, 'YYYY-MM-DD'), :percentual,
            :utilizadeccusto, :codtaborig, TO_DATE(:dtalter, 'YYYY-MM-DD'),
            :codtab, :jape_id, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )`,
          {
            idSistema,
            nutab: tabela.NUTAB,
            dtvigor: converterDataSankhya(tabela.DTVIGOR),
            percentual: tabela.PERCENTUAL || null,
            utilizadeccusto: tabela.UTILIZADECCUSTO || null,
            codtaborig: tabela.CODTABORIG || null,
            dtalter: converterDataSankhya(tabela.DTALTER),
            codtab: tabela.CODTAB || null,
            jape_id: tabela.JAPE_ID || null
          },
          { autoCommit: false }
        );
        inseridos++;
      }
    } catch (error: any) {
      console.error(`❌ [Sync] Erro ao processar tabela ${tabela.NUTAB}:`, error.message);
    }
  }

  console.log(`✅ [Sync] Upsert concluído: ${inseridos} inseridos, ${atualizados} atualizados`);
  return { inseridos, atualizados };
}

/**
 * Sincronizar tabelas de preços de uma empresa específica
 */
export async function sincronizarTabelaPrecosPorEmpresa(
  idSistema: number,
  empresaNome: string
): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE TABELAS DE PREÇOS`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    const tabelasPrecos = await buscarTabelaPrecosSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertTabelasPrecos(connection, idSistema, tabelasPrecos);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${tabelasPrecos.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_TABELA_PRECOS',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: tabelasPrecos.length,
        REGISTROS_INSERIDOS: inseridos,
        REGISTROS_ATUALIZADOS: atualizados,
        REGISTROS_DELETADOS: registrosDeletados,
        DURACAO_MS: duracao,
        DATA_INICIO: dataInicio,
        DATA_FIM: dataFim
      });
    } catch (logError) {
      console.error('❌ [Sync] Erro ao salvar log:', logError);
    }

    return {
      success: true,
      idSistema,
      empresa: empresaNome,
      totalRegistros: tabelasPrecos.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar tabelas de preços para ${empresaNome}:`, error);

    if (connection) {
      try {
        await connection.rollback();
      } catch (rollbackError) {
        console.error('❌ [Sync] Erro ao fazer rollback:', rollbackError);
      }
    }

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    // Salvar log de falha
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_TABELA_PRECOS',
        STATUS: 'FALHA',
        TOTAL_REGISTROS: 0,
        REGISTROS_INSERIDOS: 0,
        REGISTROS_ATUALIZADOS: 0,
        REGISTROS_DELETADOS: 0,
        DURACAO_MS: duracao,
        MENSAGEM_ERRO: error.message,
        DATA_INICIO: dataInicio,
        DATA_FIM: dataFim
      });
    } catch (logError) {
      console.error('❌ [Sync] Erro ao salvar log:', logError);
    }

    return {
      success: false,
      idSistema,
      empresa: empresaNome,
      totalRegistros: 0,
      registrosInseridos: 0,
      registrosAtualizados: 0,
      registrosDeletados: 0,
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
      duracao,
      erro: error.message
    };

  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Sincronizar tabelas de preços de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de tabelas de preços de todas as empresas...');

  let connection: oracledb.Connection | undefined;
  const resultados: SyncResult[] = [];

  try {
    connection = await getOracleConnection();

    const result = await connection.execute(
      `SELECT ID_EMPRESA, EMPRESA FROM AD_CONTRATOS WHERE ATIVO = 'S' ORDER BY EMPRESA`,
      [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    await connection.close();
    connection = undefined;

    if (!result.rows || result.rows.length === 0) {
      console.log('⚠️ [Sync] Nenhuma empresa ativa encontrada');
      return [];
    }

    const empresas = result.rows as any[];
    console.log(`📋 [Sync] ${empresas.length} empresas ativas encontradas`);

    for (const empresa of empresas) {
      const resultado = await sincronizarTabelaPrecosPorEmpresa(
        empresa.ID_EMPRESA,
        empresa.EMPRESA
      );
      resultados.push(resultado);

      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    const sucessos = resultados.filter(r => r.success).length;
    const falhas = resultados.filter(r => !r.success).length;

    console.log(`🏁 [Sync] Sincronização de todas as empresas concluída`);
    console.log(`✅ Sucessos: ${sucessos}, ❌ Falhas: ${falhas}`);

    return resultados;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao sincronizar todas as empresas:', error);
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}

/**
 * Obter estatísticas de sincronização
 */
export async function obterEstatisticasSincronizacao(idSistema?: number): Promise<any[]> {
  let connection: oracledb.Connection | undefined;

  try {
    connection = await getOracleConnection();

    const query = idSistema
      ? `SELECT 
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_TABELA_PRECOS
        WHERE ID_SISTEMA = :idSistema
        GROUP BY ID_SISTEMA`
      : `SELECT 
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_TABELA_PRECOS
        GROUP BY ID_SISTEMA`;

    const result = await connection.execute(
      query,
      idSistema ? [idSistema] : [],
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows as any[];

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao obter estatísticas:', error);
    throw error;
  } finally {
    if (connection) {
      try {
        await connection.close();
      } catch (closeError) {
        console.error('❌ [Sync] Erro ao fechar conexão:', closeError);
      }
    }
  }
}
