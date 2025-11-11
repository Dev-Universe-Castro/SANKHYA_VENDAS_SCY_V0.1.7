import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';

interface CabecalhoNota {
  NUNOTA: number;
  CODTIPOPER: number;
  CODTIPVENDA?: number;
  CODPARC: number;
  CODVEND?: number;
  VLRNOTA?: number;
  DTNEG?: string;
  TIPMOV: string;
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
 * Buscar cabeçalhos de nota do Sankhya
 */
async function buscarCabecalhoNotaSankhya(idSistema: number, bearerToken: string): Promise<CabecalhoNota[]> {
  console.log(`📋 [Sync] Buscando cabeçalhos de nota do Sankhya para empresa ${idSistema}...`);

  const payload = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "CabecalhoNota",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,
        "entity": {
          "fieldset": {
            "list": "NUNOTA, CODTIPOPER, CODTIPVENDA, CODPARC, CODVEND, VLRNOTA, DTNEG, TIPMOV"
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
      timeout: 60000
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
      console.log('⚠️ [Sync] Nenhum cabeçalho de nota encontrado');
      console.log('📋 [Sync] Estrutura da resposta:', JSON.stringify(response.data, null, 2));
      return [];
    }

    const entities = response.data.responseBody.entities;
    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

    const cabecalhos = entityArray.map((rawEntity: any) => {
      const cleanObject: any = {};
      for (let i = 0; i < fieldNames.length; i++) {
        const fieldKey = `f${i}`;
        const fieldName = fieldNames[i];
        if (rawEntity[fieldKey]) {
          cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
      }
      return cleanObject as CabecalhoNota;
    });

    console.log(`✅ [Sync] ${cabecalhos.length} cabeçalhos de nota encontrados`);
    return cabecalhos;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao buscar cabeçalhos de nota do Sankhya:', error.message);
    throw new Error(`Erro ao buscar cabeçalhos de nota: ${error.message}`);
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_CABECALHO_NOTA 
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
 * Upsert (inserir ou atualizar) cabeçalhos de nota
 */
async function upsertCabecalhoNota(
  connection: oracledb.Connection,
  idSistema: number,
  cabecalhos: CabecalhoNota[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  for (const cabecalho of cabecalhos) {
    try {
      const checkResult = await connection.execute(
        `SELECT COUNT(*) as count FROM AS_CABECALHO_NOTA 
         WHERE ID_SISTEMA = :idSistema AND NUNOTA = :nunota`,
        [idSistema, cabecalho.NUNOTA],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const exists = (checkResult.rows as any[])[0].COUNT > 0;

      // Função auxiliar para converter data
      const converterDataParaOracle = (dataStr: string | undefined): Date | null => {
        if (!dataStr) return null;

        try {
          const dtnegStr = String(dataStr).trim();

          // Formato ISO: YYYY-MM-DD ou YYYY-MM-DD HH:MM:SS
          if (dtnegStr.includes('-')) {
            const datePart = dtnegStr.split(' ')[0];
            const [year, month, day] = datePart.split('-').map(Number);

            if (!year || !month || !day || month < 1 || month > 12 || day < 1 || day > 31) {
              return null;
            }

            return new Date(year, month - 1, day);
          }
          // Formato brasileiro: DD/MM/YYYY
          else if (dtnegStr.includes('/')) {
            const datePart = dtnegStr.split(' ')[0];
            const [day, month, year] = datePart.split('/').map(Number);

            if (!year || !month || !day || month < 1 || month > 12 || day < 1 || day > 31) {
              return null;
            }

            return new Date(year, month - 1, day);
          }

          return null;
        } catch (error) {
          return null;
        }
      };

      if (exists) {
        const dtnegDate = converterDataParaOracle(cabecalho.DTNEG);

        await connection.execute(
          `UPDATE AS_CABECALHO_NOTA SET
            CODTIPOPER = :codtipoper,
            CODTIPVENDA = :codtipvenda,
            CODPARC = :codparc,
            CODVEND = :codvend,
            VLRNOTA = :vlrnota,
            DTNEG = :dtneg,
            TIPMOV = :tipmov,
            SANKHYA_ATUAL = 'S',
            DT_ULT_CARGA = CURRENT_TIMESTAMP
          WHERE ID_SISTEMA = :idSistema AND NUNOTA = :nunota`,
          {
            codtipoper: cabecalho.CODTIPOPER,
            codtipvenda: cabecalho.CODTIPVENDA || null,
            codparc: cabecalho.CODPARC,
            codvend: cabecalho.CODVEND || null,
            vlrnota: cabecalho.VLRNOTA || null,
            dtneg: dtnegDate,
            tipmov: cabecalho.TIPMOV,
            idSistema,
            nunota: cabecalho.NUNOTA
          },
          { autoCommit: false }
        );
        atualizados++;
      } else {
        // Função auxiliar para converter data para formato Oracle
        const converterDataParaOracle = (dataStr: string | undefined): Date | null => {
          if (!dataStr) return null;

          try {
            const dtnegStr = String(dataStr).trim();

            // Formato ISO: YYYY-MM-DD ou YYYY-MM-DD HH:MM:SS
            if (dtnegStr.includes('-')) {
              const datePart = dtnegStr.split(' ')[0];
              const [year, month, day] = datePart.split('-').map(Number);

              // Validar componentes da data
              if (!year || !month || !day || month < 1 || month > 12 || day < 1 || day > 31) {
                console.log(`⚠️ [Sync] Data inválida (ISO): ${dtnegStr}`);
                return null;
              }

              return new Date(year, month - 1, day);
            }
            // Formato brasileiro: DD/MM/YYYY
            else if (dtnegStr.includes('/')) {
              const datePart = dtnegStr.split(' ')[0];
              const [day, month, year] = datePart.split('/').map(Number);

              // Validar componentes da data
              if (!year || !month || !day || month < 1 || month > 12 || day < 1 || day > 31) {
                console.log(`⚠️ [Sync] Data inválida (BR): ${dtnegStr}`);
                return null;
              }

              return new Date(year, month - 1, day);
            }

            console.log(`⚠️ [Sync] Formato de data não reconhecido: ${dtnegStr}`);
            return null;
          } catch (error) {
            console.log(`⚠️ [Sync] Erro ao converter data: ${dataStr}`, error);
            return null;
          }
        };

        const dtnegDate = converterDataParaOracle(cabecalho.DTNEG);

        await connection.execute(
          `INSERT INTO AS_CABECALHO_NOTA (
            ID_SISTEMA, NUNOTA, CODTIPOPER, CODTIPVENDA, CODPARC,
            CODVEND, VLRNOTA, DTNEG, TIPMOV,
            SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
          ) VALUES (
            :idSistema, :nunota, :codtipoper, :codtipvenda, :codparc,
            :codvend, :vlrnota, :dtneg, :tipmov,
            'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )`,
          {
            idSistema,
            nunota: cabecalho.NUNOTA,
            codtipoper: cabecalho.CODTIPOPER || null,
            codtipvenda: cabecalho.CODTIPVENDA || null,
            codparc: cabecalho.CODPARC || null,
            codvend: cabecalho.CODVEND || null,
            vlrnota: cabecalho.VLRNOTA || null,
            dtneg: dtnegDate,
            tipmov: cabecalho.TIPMOV || null
          },
          { autoCommit: false }
        );

        inseridos++;
      }
    } catch (error: any) {
      console.error(`❌ [Sync] Erro ao processar cabeçalho NUNOTA ${cabecalho.NUNOTA}:`, error.message);
    }
  }

  console.log(`✅ [Sync] Upsert concluído: ${inseridos} inseridos, ${atualizados} atualizados`);
  return { inseridos, atualizados };
}

/**
 * Sincronizar cabeçalhos de nota de uma empresa específica
 */
export async function sincronizarCabecalhoNotaPorEmpresa(
  idSistema: number,
  empresaNome: string
): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE CABEÇALHOS DE NOTA`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    const cabecalhos = await buscarCabecalhoNotaSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertCabecalhoNota(connection, idSistema, cabecalhos);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${cabecalhos.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_CABECALHO_NOTA',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: cabecalhos.length,
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
      totalRegistros: cabecalhos.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar cabeçalhos de nota para ${empresaNome}:`, error);

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
        TABELA: 'AS_CABECALHO_NOTA',
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
 * Sincronizar cabeçalhos de nota de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de cabeçalhos de nota de todas as empresas...');

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
      const resultado = await sincronizarCabecalhoNotaPorEmpresa(
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
        FROM AS_CABECALHO_NOTA
        WHERE ID_SISTEMA = :idSistema
        GROUP BY ID_SISTEMA`
      : `SELECT 
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_CABECALHO_NOTA
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