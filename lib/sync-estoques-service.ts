
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { fazerRequisicaoAutenticada, obterToken } from './sankhya-api';

const URL_CONSULTA_SERVICO = "https://api.sandbox.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadRecords&outputType=json";

interface EstoqueSankhya {
  ESTOQUE: number;
  CODPROD: number;
  ATIVO: string;
  CONTROLE: string;
  CODLOCAL: string;
}

interface SyncResult {
  success: boolean;
  idSistema: number;
  empresa: string;
  totalRegistros: number;
  registrosInseridos: number;
  registrosAtualizados: number;
  registrosDeletados: number;
  dataInicio: Date;
  dataFim: Date;
  duracao: number;
  erro?: string;
}

/**
 * Busca todos os estoques do Sankhya para uma empresa específica
 */
async function buscarEstoquesSankhya(idSistema: number, bearerToken: string): Promise<EstoqueSankhya[]> {
  // Removido o filtro CONTROLE = 'E' para buscar TODOS os estoques
  const PAYLOAD = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "Estoque",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,
        "entity": {
          "fieldset": {
            "list": "ESTOQUE, CODPROD, ATIVO, CONTROLE, CODLOCAL"
          }
        }
        // Sem critérios - buscar todos os estoques
      }
    }
  };

  console.log(`🔍 [Sync] Buscando TODOS os estoques do Sankhya para ID_SISTEMA: ${idSistema}`);
  console.log(`📋 [Sync] Payload enviado:`, JSON.stringify(PAYLOAD, null, 2));

  const respostaCompleta = await fazerRequisicaoAutenticada(
    URL_CONSULTA_SERVICO,
    'POST',
    PAYLOAD,
    0,
    idSistema
  );

  console.log(`📦 [Sync] Resposta recebida:`, {
    hasResponseBody: !!respostaCompleta.responseBody,
    hasEntities: !!respostaCompleta.responseBody?.entities,
    total: respostaCompleta.responseBody?.entities?.total,
    status: respostaCompleta.status,
    serviceName: respostaCompleta.serviceName
  });

  const entities = respostaCompleta.responseBody?.entities;

  if (!entities || !entities.entity) {
    console.log(`⚠️ [Sync] Nenhum estoque encontrado para ID_SISTEMA: ${idSistema}`);
    console.log(`📋 [Sync] Estrutura da resposta completa:`, JSON.stringify(respostaCompleta, null, 2));
    console.log(`📋 [Sync] Entities:`, JSON.stringify(entities, null, 2));
    return [];
  }

  const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
  const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

  const estoques = entityArray.map((rawEntity: any) => {
    const cleanObject: any = {};
    for (let i = 0; i < fieldNames.length; i++) {
      const fieldKey = `f${i}`;
      const fieldName = fieldNames[i];
      if (rawEntity[fieldKey]) {
        cleanObject[fieldName] = rawEntity[fieldKey].$;
      }
    }
    return cleanObject as EstoqueSankhya;
  });

  console.log(`✅ [Sync] ${estoques.length} estoques recuperados do Sankhya`);
  return estoques;
}

/**
 * Executa o soft delete (marca como não atual) todos os estoques do sistema
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_ESTOQUES 
     SET SANKHYA_ATUAL = 'N', 
         DT_ULT_CARGA = CURRENT_TIMESTAMP 
     WHERE ID_SISTEMA = :idSistema 
       AND SANKHYA_ATUAL = 'S'`,
    { idSistema },
    { autoCommit: false }
  );

  const rowsAffected = result.rowsAffected || 0;
  console.log(`🗑️ [Sync] ${rowsAffected} registros marcados como não atuais`);
  return rowsAffected;
}

/**
 * Executa UPSERT de estoques usando MERGE
 */
async function upsertEstoques(
  connection: oracledb.Connection, 
  idSistema: number, 
  estoques: EstoqueSankhya[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  const BATCH_SIZE = 100;

  for (let i = 0; i < estoques.length; i += BATCH_SIZE) {
    const batch = estoques.slice(i, i + BATCH_SIZE);

    for (const estoque of batch) {
      // Validar e normalizar o campo CONTROLE
      // Aceita apenas 'E' (Estoque) ou 'L' (Lote), default para 'E'
      let controleValido = 'E';
      if (estoque.CONTROLE) {
        const controleUpper = estoque.CONTROLE.toUpperCase().trim();
        if (controleUpper === 'E' || controleUpper === 'L') {
          controleValido = controleUpper;
        }
      }

      const result = await connection.execute(
        `MERGE INTO AS_ESTOQUES dest
         USING (SELECT :idSistema AS ID_SISTEMA, :codProd AS CODPROD, :codLocal AS CODLOCAL FROM DUAL) src
         ON (dest.ID_SISTEMA = src.ID_SISTEMA AND dest.CODPROD = src.CODPROD AND dest.CODLOCAL = src.CODLOCAL)
         WHEN MATCHED THEN
           UPDATE SET
             ESTOQUE = :estoque,
             ATIVO = :ativo,
             CONTROLE = :controle,
             SANKHYA_ATUAL = 'S',
             DT_ULT_CARGA = CURRENT_TIMESTAMP
         WHEN NOT MATCHED THEN
           INSERT (
             ID_SISTEMA, CODPROD, CODLOCAL, ESTOQUE, ATIVO, CONTROLE, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
           )
           VALUES (
             :idSistema, :codProd, :codLocal, :estoque, :ativo, :controle, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           )`,
        {
          idSistema,
          codProd: estoque.CODPROD || null,
          codLocal: estoque.CODLOCAL || null,
          estoque: estoque.ESTOQUE || 0,
          ativo: estoque.ATIVO || 'S',
          controle: controleValido
        },
        { autoCommit: false }
      );

      if (result.rowsAffected && result.rowsAffected > 0) {
        const checkResult = await connection.execute(
          `SELECT DT_CRIACAO FROM AS_ESTOQUES 
           WHERE ID_SISTEMA = :idSistema AND CODPROD = :codProd AND CODLOCAL = :codLocal`,
          { idSistema, codProd: estoque.CODPROD, codLocal: estoque.CODLOCAL },
          { outFormat: oracledb.OUT_FORMAT_OBJECT }
        );

        if (checkResult.rows && checkResult.rows.length > 0) {
          const row: any = checkResult.rows[0];
          const dtCriacao = new Date(row.DT_CRIACAO);
          const agora = new Date();
          const diferencaMs = agora.getTime() - dtCriacao.getTime();

          if (diferencaMs < 5000) {
            inseridos++;
          } else {
            atualizados++;
          }
        }
      }
    }

    await connection.commit();
    console.log(`📦 [Sync] Processado lote ${Math.floor(i / BATCH_SIZE) + 1} de ${Math.ceil(estoques.length / BATCH_SIZE)}`);
  }

  console.log(`✅ [Sync] UPSERT concluído - Inseridos: ${inseridos}, Atualizados: ${atualizados}`);
  return { inseridos, atualizados };
}

/**
 * Sincroniza estoques de uma empresa específica
 */
export async function sincronizarEstoquesPorEmpresa(idSistema: number, empresaNome: string): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE ESTOQUES`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);
    
    // SEMPRE forçar renovação do token para garantir credenciais corretas
    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    
    const estoques = await buscarEstoquesSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertEstoques(connection, idSistema, estoques);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${estoques.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_ESTOQUES',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: estoques.length,
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
      totalRegistros: estoques.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio,
      dataFim,
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar estoques para ${empresaNome}:`, error);

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
        TABELA: 'AS_ESTOQUES',
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
      dataInicio,
      dataFim,
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
 * Sincroniza estoques de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de estoques de todas as empresas...');

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
      const resultado = await sincronizarEstoquesPorEmpresa(
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
export async function obterEstatisticasSincronizacao(idSistema?: number) {
  const connection = await getOracleConnection();

  try {
    const whereClause = idSistema ? `WHERE ID_SISTEMA = :idSistema` : '';
    const params = idSistema ? { idSistema } : {};

    const result = await connection.execute(
      `SELECT 
         ID_SISTEMA,
         COUNT(*) as TOTAL_REGISTROS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
         SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN ESTOQUE ELSE 0 END) as ESTOQUE_TOTAL,
         MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
       FROM AS_ESTOQUES
       ${whereClause}
       GROUP BY ID_SISTEMA
       ORDER BY ID_SISTEMA`,
      params,
      { outFormat: oracledb.OUT_FORMAT_OBJECT }
    );

    return result.rows || [];
  } finally {
    await connection.close();
  }
}
