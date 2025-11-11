
import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import axios from 'axios';

interface Financeiro {
  NUFIN: number;
  CODPARC?: number;
  CODEMP?: number;
  VLRDESDOB?: number;
  DTVENC?: string;
  DTNEG?: string;
  PROVISAO?: string;
  DHBAIXA?: string;
  VLRBAIXA?: number;
  RECDESP?: number;
  NOSSONUM?: string;
  CODCTABCOINT?: number;
  HISTORICO?: string;
  NUMNOTA?: number;
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
 * Buscar títulos financeiros do Sankhya
 */
async function buscarFinanceiroSankhya(idSistema: number, bearerToken: string): Promise<Financeiro[]> {
  console.log(`📋 [Sync] Buscando títulos financeiros do Sankhya para empresa ${idSistema}...`);

  const payload = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "Financeiro",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,
        "entity": {
          "fieldset": {
            "list": "NUFIN, CODPARC, CODEMP, VLRDESDOB, DTVENC, DTNEG, PROVISAO, DHBAIXA, VLRBAIXA, RECDESP, NOSSONUM, CODCTABCOINT, HISTORICO, NUMNOTA"
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
      console.log('⚠️ [Sync] Nenhum título financeiro encontrado');
      console.log('📋 [Sync] Estrutura da resposta:', JSON.stringify(response.data, null, 2));
      return [];
    }

    const entities = response.data.responseBody.entities;
    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

    const financeiros = entityArray.map((rawEntity: any) => {
      const cleanObject: any = {};
      for (let i = 0; i < fieldNames.length; i++) {
        const fieldKey = `f${i}`;
        const fieldName = fieldNames[i];
        if (rawEntity[fieldKey]) {
          cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
      }
      return cleanObject as Financeiro;
    });

    console.log(`✅ [Sync] ${financeiros.length} títulos financeiros encontrados`);
    return financeiros;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao buscar títulos financeiros do Sankhya:', error.message);
    throw new Error(`Erro ao buscar títulos financeiros: ${error.message}`);
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_FINANCEIRO 
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
 * Converter data do formato Sankhya para Date do Oracle
 */
function parseDataSankhya(dataStr: string | undefined): Date | null {
  if (!dataStr) return null;
  
  try {
    // Formato esperado: DD/MM/YYYY ou DD/MM/YYYY HH:MI:SS
    const partes = dataStr.trim().split(' ');
    const dataParte = partes[0];
    const horaParte = partes[1] || '00:00:00';
    
    const [dia, mes, ano] = dataParte.split('/');
    
    if (!dia || !mes || !ano) {
      return null;
    }
    
    const [hora, minuto, segundo] = horaParte.split(':');
    
    const date = new Date(
      parseInt(ano),
      parseInt(mes) - 1,
      parseInt(dia),
      parseInt(hora || '0'),
      parseInt(minuto || '0'),
      parseInt(segundo || '0')
    );
    
    if (isNaN(date.getTime())) {
      return null;
    }
    
    return date;
  } catch (error) {
    return null;
  }
}

/**
 * Validar e limitar valor numérico
 */
function validarValorNumerico(valor: number | undefined, maxDigits: number = 15): number | null {
  if (valor === undefined || valor === null) return null;
  
  const valorNum = Number(valor);
  if (isNaN(valorNum)) return null;
  
  // Limitar a 15 dígitos totais (para NUMBER(15,2))
  const maxValue = Math.pow(10, maxDigits - 2) - 0.01;
  if (Math.abs(valorNum) > maxValue) {
    console.warn(`⚠️ [Sync] Valor ${valorNum} excede precisão máxima, será limitado a ${maxValue}`);
    return valorNum > 0 ? maxValue : -maxValue;
  }
  
  return valorNum;
}

/**
 * Upsert (inserir ou atualizar) títulos financeiros
 */
async function upsertFinanceiro(
  connection: oracledb.Connection,
  idSistema: number,
  financeiros: Financeiro[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  for (const financeiro of financeiros) {
    try {
      // Converter datas
      const dtvenc = parseDataSankhya(financeiro.DTVENC);
      const dtneg = parseDataSankhya(financeiro.DTNEG);
      const dhbaixa = parseDataSankhya(financeiro.DHBAIXA);
      
      // Validar valores numéricos
      const vlrdesdob = validarValorNumerico(financeiro.VLRDESDOB);
      const vlrbaixa = validarValorNumerico(financeiro.VLRBAIXA);
      
      const checkResult = await connection.execute(
        `SELECT COUNT(*) as count FROM AS_FINANCEIRO 
         WHERE ID_SISTEMA = :idSistema AND NUFIN = :nufin`,
        [idSistema, financeiro.NUFIN],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const exists = (checkResult.rows as any[])[0].COUNT > 0;

      if (exists) {
        await connection.execute(
          `UPDATE AS_FINANCEIRO SET
            CODPARC = :codparc,
            CODEMP = :codemp,
            VLRDESDOB = :vlrdesdob,
            DTVENC = :dtvenc,
            DTNEG = :dtneg,
            PROVISAO = :provisao,
            DHBAIXA = :dhbaixa,
            VLRBAIXA = :vlrbaixa,
            RECDESP = :recdesp,
            NOSSONUM = :nossonum,
            CODCTABCOINT = :codctabcoint,
            HISTORICO = :historico,
            NUMNOTA = :numnota,
            SANKHYA_ATUAL = 'S',
            DT_ULT_CARGA = CURRENT_TIMESTAMP
          WHERE ID_SISTEMA = :idSistema AND NUFIN = :nufin`,
          {
            codparc: financeiro.CODPARC || null,
            codemp: financeiro.CODEMP || null,
            vlrdesdob,
            dtvenc,
            dtneg,
            provisao: financeiro.PROVISAO || null,
            dhbaixa,
            vlrbaixa,
            recdesp: financeiro.RECDESP || null,
            nossonum: financeiro.NOSSONUM || null,
            codctabcoint: financeiro.CODCTABCOINT || null,
            historico: financeiro.HISTORICO || null,
            numnota: financeiro.NUMNOTA || null,
            idSistema,
            nufin: financeiro.NUFIN
          },
          { autoCommit: false }
        );
        atualizados++;
      } else {
        await connection.execute(
          `INSERT INTO AS_FINANCEIRO (
            ID_SISTEMA, NUFIN, CODPARC, CODEMP, VLRDESDOB, DTVENC, DTNEG,
            PROVISAO, DHBAIXA, VLRBAIXA, RECDESP, NOSSONUM, CODCTABCOINT,
            HISTORICO, NUMNOTA, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
          ) VALUES (
            :idSistema, :nufin, :codparc, :codemp, :vlrdesdob, :dtvenc, :dtneg,
            :provisao, :dhbaixa, :vlrbaixa, :recdesp, :nossonum, :codctabcoint,
            :historico, :numnota, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )`,
          {
            idSistema,
            nufin: financeiro.NUFIN,
            codparc: financeiro.CODPARC || null,
            codemp: financeiro.CODEMP || null,
            vlrdesdob,
            dtvenc,
            dtneg,
            provisao: financeiro.PROVISAO || null,
            dhbaixa,
            vlrbaixa,
            recdesp: financeiro.RECDESP || null,
            nossonum: financeiro.NOSSONUM || null,
            codctabcoint: financeiro.CODCTABCOINT || null,
            historico: financeiro.HISTORICO || null,
            numnota: financeiro.NUMNOTA || null
          },
          { autoCommit: false }
        );
        inseridos++;
      }
    } catch (error: any) {
      console.error(`❌ [Sync] Erro ao processar financeiro NUFIN ${financeiro.NUFIN}:`, error.message);
    }
  }

  console.log(`✅ [Sync] Upsert concluído: ${inseridos} inseridos, ${atualizados} atualizados`);
  return { inseridos, atualizados };
}

/**
 * Sincronizar títulos financeiros de uma empresa específica
 */
export async function sincronizarFinanceiroPorEmpresa(
  idSistema: number,
  empresaNome: string
): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE FINANCEIRO`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    const financeiros = await buscarFinanceiroSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertFinanceiro(connection, idSistema, financeiros);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${financeiros.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    try {
      const { salvarLogSincronizacao } = await import('./sync-logs-service');
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_FINANCEIRO',
        STATUS: 'SUCESSO',
        TOTAL_REGISTROS: financeiros.length,
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
      totalRegistros: financeiros.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro ao sincronizar financeiro para ${empresaNome}:`, error);

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
        TABELA: 'AS_FINANCEIRO',
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
 * Sincronizar financeiro de todas as empresas ativas (uma por vez)
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de financeiro de todas as empresas...');

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
      const resultado = await sincronizarFinanceiroPorEmpresa(
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
        FROM AS_FINANCEIRO
        WHERE ID_SISTEMA = :idSistema
        GROUP BY ID_SISTEMA`
      : `SELECT 
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_FINANCEIRO
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
