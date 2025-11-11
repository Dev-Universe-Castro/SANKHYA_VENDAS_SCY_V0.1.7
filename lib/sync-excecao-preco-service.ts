
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';

interface ExcecaoPreco {
  CODPROD: number;
  VLRANT?: number;
  VARIACAO?: number;
  NUTAB: number;
  TIPO?: string;
  VLRVENDA?: number;
  CODLOCAL: number;
  CONTROLE?: string;
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
 * Buscar exceções de preço do Sankhya
 */
async function buscarExcecaoPrecoSankhya(idSistema: number, bearerToken: string): Promise<ExcecaoPreco[]> {
  console.log(`📋 [Sync] Buscando exceções de preço do Sankhya para empresa ${idSistema}...`);

  const payload = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "Excecao",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,
        "entity": {
          "fieldset": {
            "list": "CODPROD, VLRANT, VARIACAO, NUTAB, TIPO, VLRVENDA, CODLOCAL, CONTROLE"
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

    console.log('📦 [Sync] Resposta completa da API:', {
      hasData: !!response.data,
      hasResponseBody: !!response.data?.responseBody,
      hasEntities: !!response.data?.responseBody?.entities,
      hasEntity: !!response.data?.responseBody?.entities?.entity,
      total: response.data?.responseBody?.entities?.total,
      status: response.data?.status,
      statusMessage: response.data?.statusMessage
    });

    if (!response.data?.responseBody?.entities?.entity) {
      console.log('⚠️ [Sync] Nenhuma exceção de preço encontrada');
      console.log('📋 [Sync] Estrutura da resposta:', JSON.stringify(response.data, null, 2));
      return [];
    }

    const entities = response.data.responseBody.entities;
    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

    const excecoes = entityArray.map((rawEntity: any) => {
      const cleanObject: any = {};
      for (let i = 0; i < fieldNames.length; i++) {
        const fieldKey = `f${i}`;
        const fieldName = fieldNames[i];
        if (rawEntity[fieldKey]) {
          cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
      }
      return cleanObject as ExcecaoPreco;
    });

    console.log(`✅ [Sync] ${excecoes.length} exceções de preço encontradas`);
    return excecoes;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao buscar exceções de preço do Sankhya:', error.message);
    throw new Error(`Erro ao buscar exceções de preço: ${error.message}`);
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_EXCECAO_PRECO 
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
 * Upsert (inserir ou atualizar) exceções de preço
 */
async function upsertExcecaoPreco(
  connection: oracledb.Connection,
  idSistema: number,
  excecoes: ExcecaoPreco[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  for (const excecao of excecoes) {
    try {
      const checkResult = await connection.execute(
        `SELECT COUNT(*) as count FROM AS_EXCECAO_PRECO 
         WHERE ID_SISTEMA = :idSistema AND CODPROD = :codprod AND NUTAB = :nutab AND CODLOCAL = :codlocal`,
        [idSistema, excecao.CODPROD, excecao.NUTAB, excecao.CODLOCAL],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const exists = (checkResult.rows as any[])[0].COUNT > 0;

      if (exists) {
        await connection.execute(
          `UPDATE AS_EXCECAO_PRECO SET
            VLRANT = :vlrant,
            VARIACAO = :variacao,
            TIPO = :tipo,
            VLRVENDA = :vlrvenda,
            CONTROLE = :controle,
            SANKHYA_ATUAL = 'S',
            DT_ULT_CARGA = CURRENT_TIMESTAMP
          WHERE ID_SISTEMA = :idSistema AND CODPROD = :codprod AND NUTAB = :nutab AND CODLOCAL = :codlocal`,
          {
            vlrant: excecao.VLRANT || null,
            variacao: excecao.VARIACAO || null,
            tipo: excecao.TIPO || null,
            vlrvenda: excecao.VLRVENDA || null,
            controle: excecao.CONTROLE || null,
            idSistema,
            codprod: excecao.CODPROD,
            nutab: excecao.NUTAB,
            codlocal: excecao.CODLOCAL
          },
          { autoCommit: false }
        );
        atualizados++;
      } else {
        await connection.execute(
          `INSERT INTO AS_EXCECAO_PRECO (
            ID_SISTEMA, CODPROD, NUTAB, CODLOCAL,
            VLRANT, VARIACAO, TIPO, VLRVENDA, CONTROLE,
            SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
          ) VALUES (
            :idSistema, :codprod, :nutab, :codlocal, 
            :vlrant, :variacao, :tipo, :vlrvenda,
            :controle, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )`,
          {
            idSistema,
            codprod: excecao.CODPROD,
            nutab: excecao.NUTAB,
            codlocal: excecao.CODLOCAL,
            vlrant: excecao.VLRANT || null,
            variacao: excecao.VARIACAO || null,
            tipo: excecao.TIPO || null,
            vlrvenda: excecao.VLRVENDA || null,
            controle: excecao.CONTROLE || null
          },
          { autoCommit: false }
        );
        inseridos++;
      }
    } catch (error: any) {
      console.error(`❌ [Sync] Erro ao processar exceção CODPROD ${excecao.CODPROD}:`, error.message);
    }
  }

  console.log(`✅ [Sync] Upsert concluído: ${inseridos} inseridos, ${atualizados} atualizados`);
  return { inseridos, atualizados };
}

/**
 * Sincronizar exceções de preço de uma empresa específica
 */
export async function sincronizarExcecaoPrecoPorEmpresa(
  idSistema: number,
  empresaNome: string
): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE EXCEÇÕES DE PREÇO`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    const excecoes = await buscarExcecaoPrecoSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertExcecaoPreco(connection, idSistema, excecoes);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${excecoes.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_EXCECAO_PRECO',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: excecoes.length,
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
      totalRegistros: excecoes.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar exceções de preço para ${empresaNome}:`, error);

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
        TABELA: 'AS_EXCECAO_PRECO',
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
 * Sincronizar exceções de preço de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de exceções de preço de todas as empresas...');

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
      const resultado = await sincronizarExcecaoPrecoPorEmpresa(
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
        FROM AS_EXCECAO_PRECO
        WHERE ID_SISTEMA = :idSistema
        GROUP BY ID_SISTEMA`
      : `SELECT 
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_EXCECAO_PRECO
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
