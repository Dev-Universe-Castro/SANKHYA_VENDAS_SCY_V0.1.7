import oracledb from 'oracledb';
import { getOracleConnection } from './oracle-service';
import { obterToken } from './sankhya-api';
import { listarContratos } from './oracle-service';
import { salvarLogSincronizacao } from './sync-logs-service';
import axios from 'axios';

interface Vendedor {
  CODVEND: number;
  APELIDO?: string;
  ATIVO?: string;
  ATUACOMPRADOR?: string;
  CODCARGAHOR?: number;
  CODCENCUSPAD?: number;
  CODEMP?: number;
  CODFORM?: number;
  CODFUNC?: number;
  CODGER?: number;
  CODPARC?: number;
  CODREG?: number;
  CODUSU?: number;
  COMCM?: string;
  COMGER?: number;
  COMVENDA?: number;
  DESCMAX?: number;
  DIACOM?: number;
  DTALTER?: string;
  EMAIL?: string;
  GRUPODESCVEND?: string;
  GRUPORETENCAO?: string;
  PARTICMETA?: number;
  PERCCUSVAR?: number;
  PROVACRESC?: number;
  PROVACRESCCAC?: number;
  RECHREXTRA?: string;
  SALDODISP?: number;
  SALDODISPCAC?: number;
  SENHA?: number;
  TIPCALC?: string;
  TIPFECHCOM?: string;
  TIPOCERTIF?: string;
  TIPVALOR?: string;
  TIPVEND?: string;
  VLRHORA?: number;
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
 * Buscar vendedores do Sankhya
 */
async function buscarVendedoresSankhya(idSistema: number, bearerToken: string): Promise<Vendedor[]> {
  console.log(`📋 [Sync] Buscando vendedores do Sankhya para empresa ${idSistema}...`);

  const payload = {
    "requestBody": {
      "dataSet": {
        "rootEntity": "Vendedor",
        "includePresentationFields": "N",
        "offsetPage": null,
        "disableRowsLimit": true,
        "entity": {
          "fieldset": {
            "list": "CODVEND,APELIDO,ATIVO,ATUACOMPRADOR,CODCARGAHOR,CODCENCUSPAD,CODEMP,CODFORM,CODFUNC,CODGER,CODPARC,CODREG,CODUSU,COMCM,COMGER,COMVENDA,DESCMAX,DIACOM,DTALTER,EMAIL,GRUPODESCVEND,GRUPORETENCAO,PARTICMETA,PERCCUSVAR,PROVACRESC,PROVACRESCCAC,RECHREXTRA,SALDODISP,SALDODISPCAC,SENHA,TIPCALC,TIPFECHCOM,TIPOCERTIF,TIPVALOR,TIPVEND,VLRHORA"
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

    if (!response.data?.responseBody?.entities?.entity) {
      console.log('⚠️ [Sync] Nenhum vendedor encontrado');
      return [];
    }

    const entities = response.data.responseBody.entities;
    const fieldNames = entities.metadata.fields.field.map((f: any) => f.name);
    const entityArray = Array.isArray(entities.entity) ? entities.entity : [entities.entity];

    const vendedores = entityArray.map((rawEntity: any) => {
      const cleanObject: any = {};
      for (let i = 0; i < fieldNames.length; i++) {
        const fieldKey = `f${i}`;
        const fieldName = fieldNames[i];
        if (rawEntity[fieldKey]) {
          cleanObject[fieldName] = rawEntity[fieldKey].$;
        }
      }
      return cleanObject as Vendedor;
    });

    console.log(`✅ [Sync] ${vendedores.length} vendedores encontrados`);
    return vendedores;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao buscar vendedores do Sankhya:', error.message);
    throw new Error(`Erro ao buscar vendedores: ${error.message}`);
  }
}

/**
 * Marcar todos os registros como não atuais (soft delete)
 */
async function marcarTodosComoNaoAtuais(connection: oracledb.Connection, idSistema: number): Promise<number> {
  const result = await connection.execute(
    `UPDATE AS_VENDEDORES
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

  const maxValue = Math.pow(10, maxDigits - 2) - 0.01;
  if (Math.abs(valorNum) > maxValue) {
    console.warn(`⚠️ [Sync] Valor ${valorNum} excede precisão máxima, será limitado a ${maxValue}`);
    return valorNum > 0 ? maxValue : -maxValue;
  }

  return valorNum;
}

/**
 * Upsert (inserir ou atualizar) vendedores
 */
async function upsertVendedores(
  connection: oracledb.Connection,
  idSistema: number,
  vendedores: Vendedor[]
): Promise<{ inseridos: number; atualizados: number }> {
  let inseridos = 0;
  let atualizados = 0;

  for (const vendedor of vendedores) {
    try {
      const dtalter = parseDataSankhya(vendedor.DTALTER);

      const checkResult = await connection.execute(
        `SELECT COUNT(*) as count FROM AS_VENDEDORES
         WHERE ID_SISTEMA = :idSistema AND CODVEND = :codvend`,
        [idSistema, vendedor.CODVEND],
        { outFormat: oracledb.OUT_FORMAT_OBJECT }
      );

      const exists = (checkResult.rows as any[])[0].COUNT > 0;

      if (exists) {
        await connection.execute(
          `UPDATE AS_VENDEDORES SET
            APELIDO = :apelido,
            ATIVO = :ativo,
            ATUACOMPRADOR = :atuacomprador,
            CODCARGAHOR = :codcargahor,
            CODCENCUSPAD = :codcencuspad,
            CODEMP = :codemp,
            CODFORM = :codform,
            CODFUNC = :codfunc,
            CODGER = :codger,
            CODPARC = :codparc,
            CODREG = :codreg,
            CODUSU = :codusu,
            COMCM = :comcm,
            COMGER = :comger,
            COMVENDA = :comvenda,
            DESCMAX = :descmax,
            DIACOM = :diacom,
            DTALTER = :dtalter,
            EMAIL = :email,
            GRUPODESCVEND = :grupodescvend,
            GRUPORETENCAO = :gruporetencao,
            PARTICMETA = :particmeta,
            PERCCUSVAR = :perccusvar,
            PROVACRESC = :provacresc,
            PROVACRESCCAC = :provacresccac,
            RECHREXTRA = :rechrextra,
            SALDODISP = :saldodisp,
            SALDODISPCAC = :saldodispcac,
            SENHA = :senha,
            TIPCALC = :tipcalc,
            TIPFECHCOM = :tipfechcom,
            TIPOCERTIF = :tipocertif,
            TIPVALOR = :tipvalor,
            TIPVEND = :tipvend,
            VLRHORA = :vlrhora,
            SANKHYA_ATUAL = 'S',
            DT_ULT_CARGA = CURRENT_TIMESTAMP
          WHERE ID_SISTEMA = :idSistema AND CODVEND = :codvend`,
          {
            apelido: vendedor.APELIDO || null,
            ativo: vendedor.ATIVO || null,
            atuacomprador: vendedor.ATUACOMPRADOR || null,
            codcargahor: vendedor.CODCARGAHOR || null,
            codcencuspad: vendedor.CODCENCUSPAD || null,
            codemp: vendedor.CODEMP || null,
            codform: vendedor.CODFORM || null,
            codfunc: vendedor.CODFUNC || null,
            codger: vendedor.CODGER || null,
            codparc: vendedor.CODPARC || null,
            codreg: vendedor.CODREG || null,
            codusu: vendedor.CODUSU || null,
            comcm: vendedor.COMCM || null,
            comger: validarValorNumerico(vendedor.COMGER),
            comvenda: validarValorNumerico(vendedor.COMVENDA),
            descmax: validarValorNumerico(vendedor.DESCMAX),
            diacom: vendedor.DIACOM || null,
            dtalter,
            email: vendedor.EMAIL || null,
            grupodescvend: vendedor.GRUPODESCVEND || null,
            gruporetencao: vendedor.GRUPORETENCAO || null,
            particmeta: validarValorNumerico(vendedor.PARTICMETA),
            perccusvar: validarValorNumerico(vendedor.PERCCUSVAR),
            provacresc: validarValorNumerico(vendedor.PROVACRESC),
            provacresccac: validarValorNumerico(vendedor.PROVACRESCCAC),
            rechrextra: vendedor.RECHREXTRA || null,
            saldodisp: validarValorNumerico(vendedor.SALDODISP),
            saldodispcac: validarValorNumerico(vendedor.SALDODISPCAC),
            senha: vendedor.SENHA || null,
            tipcalc: vendedor.TIPCALC || null,
            tipfechcom: vendedor.TIPFECHCOM || null,
            tipocertif: vendedor.TIPOCERTIF || null,
            tipvalor: vendedor.TIPVALOR || null,
            tipvend: vendedor.TIPVEND || null,
            vlrhora: validarValorNumerico(vendedor.VLRHORA),
            idSistema,
            codvend: vendedor.CODVEND
          },
          { autoCommit: false }
        );
        atualizados++;
      } else {
        await connection.execute(
          `INSERT INTO AS_VENDEDORES (
            ID_SISTEMA, CODVEND, APELIDO, ATIVO, ATUACOMPRADOR, CODCARGAHOR, CODCENCUSPAD,
            CODEMP, CODFORM, CODFUNC, CODGER, CODPARC, CODREG, CODUSU, COMCM, COMGER,
            COMVENDA, DESCMAX, DIACOM, DTALTER, EMAIL, GRUPODESCVEND, GRUPORETENCAO,
            PARTICMETA, PERCCUSVAR, PROVACRESC, PROVACRESCCAC, RECHREXTRA, SALDODISP,
            SALDODISPCAC, SENHA, TIPCALC, TIPFECHCOM, TIPOCERTIF, TIPVALOR, TIPVEND,
            VLRHORA, SANKHYA_ATUAL, DT_ULT_CARGA, DT_CRIACAO
          ) VALUES (
            :idSistema, :codvend, :apelido, :ativo, :atuacomprador, :codcargahor, :codcencuspad,
            :codemp, :codform, :codfunc, :codger, :codparc, :codreg, :codusu, :comcm, :comger,
            :comvenda, :descmax, :diacom, :dtalter, :email, :grupodescvend, :gruporetencao,
            :particmeta, :perccusvar, :provacresc, :provacresccac, :rechrextra, :saldodisp,
            :saldodispcac, :senha, :tipcalc, :tipfechcom, :tipocertif, :tipvalor, :tipvend,
            :vlrhora, 'S', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )`,
          {
            idSistema,
            codvend: vendedor.CODVEND,
            apelido: vendedor.APELIDO || null,
            ativo: vendedor.ATIVO || null,
            atuacomprador: vendedor.ATUACOMPRADOR || null,
            codcargahor: vendedor.CODCARGAHOR || null,
            codcencuspad: vendedor.CODCENCUSPAD || null,
            codemp: vendedor.CODEMP || null,
            codform: vendedor.CODFORM || null,
            codfunc: vendedor.CODFUNC || null,
            codger: vendedor.CODGER || null,
            codparc: vendedor.CODPARC || null,
            codreg: vendedor.CODREG || null,
            codusu: vendedor.CODUSU || null,
            comcm: vendedor.COMCM || null,
            comger: validarValorNumerico(vendedor.COMGER),
            comvenda: validarValorNumerico(vendedor.COMVENDA),
            descmax: validarValorNumerico(vendedor.DESCMAX),
            diacom: vendedor.DIACOM || null,
            dtalter,
            email: vendedor.EMAIL || null,
            grupodescvend: vendedor.GRUPODESCVEND || null,
            gruporetencao: vendedor.GRUPORETENCAO || null,
            particmeta: validarValorNumerico(vendedor.PARTICMETA),
            perccusvar: validarValorNumerico(vendedor.PERCCUSVAR),
            provacresc: validarValorNumerico(vendedor.PROVACRESC),
            provacresccac: validarValorNumerico(vendedor.PROVACRESCCAC),
            rechrextra: vendedor.RECHREXTRA || null,
            saldodisp: validarValorNumerico(vendedor.SALDODISP),
            saldodispcac: validarValorNumerico(vendedor.SALDODISPCAC),
            senha: vendedor.SENHA || null,
            tipcalc: vendedor.TIPCALC || null,
            tipfechcom: vendedor.TIPFECHCOM || null,
            tipocertif: vendedor.TIPOCERTIF || null,
            tipvalor: vendedor.TIPVALOR || null,
            tipvend: vendedor.TIPVEND || null,
            vlrhora: validarValorNumerico(vendedor.VLRHORA)
          },
          { autoCommit: false }
        );
        inseridos++;
      }
    } catch (error: any) {
      console.error(`❌ [Sync] Erro ao processar vendedor CODVEND ${vendedor.CODVEND}:`, error.message);
      // Tentar salvar log mesmo em caso de erro no upsert de um item
      try {
        await salvarLogSincronizacao({
          ID_SISTEMA: idSistema,
          EMPRESA: empresaNome, // EmpresaName is not available here, consider passing it or logging without it
          TABELA: 'AS_VENDEDORES',
          STATUS: 'FALHA',
          TOTAL_REGISTROS: 0, // Could be improved by tracking total processed so far
          REGISTROS_INSERIDOS: 0, // Could be improved by tracking inserted so far
          REGISTROS_ATUALIZADOS: 0, // Could be improved by tracking updated so far
          REGISTROS_DELETADOS: 0, // Not applicable here
          DURACAO_MS: 0, // Not applicable here
          MENSAGEM_ERRO: `Erro ao processar vendedor CODVEND ${vendedor.CODVEND}: ${error.message}`,
          DATA_INICIO: dataInicio, // dataInicio is available
          DATA_FIM: new Date() // Current date/time
        });
      } catch (logError) {
        console.error('❌ Erro ao salvar log do item:', logError);
      }
    }
  }

  console.log(`✅ [Sync] Upsert concluído: ${inseridos} inseridos, ${atualizados} atualizados`);
  return { inseridos, atualizados };
}

/**
 * Sincronizar vendedores de uma empresa específica
 */
export async function sincronizarVendedoresPorEmpresa(
  idSistema: number,
  empresaNome: string
): Promise<SyncResult> {
  const dataInicio = new Date();
  let connection: oracledb.Connection | undefined;

  try {
    console.log(`\n🚀🚀🚀 ================================================`);
    console.log(`🚀 SINCRONIZAÇÃO DE VENDEDORES`);
    console.log(`🚀 ID_SISTEMA: ${idSistema}`);
    console.log(`🚀 Empresa: ${empresaNome}`);
    console.log(`🚀 ================================================\n`);

    console.log(`🔄 [Sync] Forçando renovação do token para contrato ${idSistema}...`);
    const bearerToken = await obterToken(idSistema, true);
    const vendedores = await buscarVendedoresSankhya(idSistema, bearerToken);
    connection = await getOracleConnection();

    const registrosDeletados = await marcarTodosComoNaoAtuais(connection, idSistema);
    const { inseridos, atualizados } = await upsertVendedores(connection, idSistema, vendedores);

    await connection.commit();

    const dataFim = new Date();
    const duracao = dataFim.getTime() - dataInicio.getTime();

    console.log(`✅ [Sync] Sincronização concluída com sucesso para ${empresaNome}`);
    console.log(`📊 [Sync] Resumo: ${vendedores.length} registros, ${inseridos} inseridos, ${atualizados} atualizados, ${registrosDeletados} deletados`);
    console.log(`⏱️ [Sync] Duração: ${duracao}ms`);

    // Salvar log de sucesso
    await salvarLogSincronizacao({
      ID_SISTEMA: idSistema,
      EMPRESA: empresaNome,
      TABELA: 'AS_VENDEDORES',
      STATUS: 'SUCESSO',
      TOTAL_REGISTROS: vendedores.length,
      REGISTROS_INSERIDOS: inseridos,
      REGISTROS_ATUALIZADOS: atualizados,
      REGISTROS_DELETADOS: registrosDeletados,
      DURACAO_MS: duracao,
      DATA_INICIO: dataInicio,
      DATA_FIM: dataFim
    });

    return {
      success: true,
      idSistema,
      empresa: empresaNome,
      totalRegistros: vendedores.length,
      registrosInseridos: inseridos,
      registrosAtualizados: atualizados,
      registrosDeletados,
      dataInicio: dataInicio.toISOString(),
      dataFim: dataFim.toISOString(),
      duracao
    };

  } catch (error: any) {
    console.error(`❌ [Sync] Erro na sincronização para ${empresaNome}:`, error.message);
    console.log(`⏱️ [Sync] Duração até erro: ${new Date().getTime() - dataInicio.getTime()}ms`);

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
      await salvarLogSincronizacao({
        ID_SISTEMA: idSistema,
        EMPRESA: empresaNome,
        TABELA: 'AS_VENDEDORES',
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
 * Sincronizar vendedores de todas as empresas ativas
 */
export async function sincronizarTodasEmpresas(): Promise<SyncResult[]> {
  console.log('🌐 [Sync] Iniciando sincronização de vendedores de todas as empresas...');

  let connection: oracledb.Connection | undefined;
  const resultados: SyncResult[] = [];
  const dataInicioGeral = new Date();
  let sucessoGeral = true;

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
      const resultado = await sincronizarVendedoresPorEmpresa(
        empresa.ID_EMPRESA,
        empresa.EMPRESA
      );
      resultados.push(resultado);
      if (!resultado.success) {
        sucessoGeral = false;
      }

      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    const sucessos = resultados.filter(r => r.success).length;
    const falhas = resultados.filter(r => !r.success).length;

    console.log(`🏁 [Sync] Sincronização de todas as empresas concluída`);
    console.log(`✅ Sucessos: ${sucessos}, ❌ Falhas: ${falhas}`);

    // Salvar log geral da sincronização
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: 0, // Geral, sem sistema específico
        EMPRESA: 'TODAS',
        TABELA: 'AS_VENDEDORES',
        STATUS: sucessoGeral ? 'SUCESSO' : 'FALHA',
        TOTAL_REGISTROS: resultados.reduce((sum, r) => sum + r.totalRegistros, 0),
        REGISTROS_INSERIDOS: resultados.reduce((sum, r) => sum + r.registrosInseridos, 0),
        REGISTROS_ATUALIZADOS: resultados.reduce((sum, r) => sum + r.registrosAtualizados, 0),
        REGISTROS_DELETADOS: resultados.reduce((sum, r) => sum + r.registrosDeletados, 0),
        DURACAO_MS: new Date().getTime() - dataInicioGeral.getTime(),
        MENSAGEM_ERRO: sucessoGeral ? null : 'Algumas sincronizações falharam. Verifique os logs individuais.',
        DATA_INICIO: dataInicioGeral,
        DATA_FIM: new Date()
      });
    } catch (logError) {
      console.error('❌ Erro ao salvar log geral:', logError);
    }

    return resultados;

  } catch (error: any) {
    console.error('❌ [Sync] Erro ao sincronizar todas as empresas:', error);
    // Tentar salvar log geral de erro
    try {
      await salvarLogSincronizacao({
        ID_SISTEMA: 0,
        EMPRESA: 'TODAS',
        TABELA: 'AS_VENDEDORES',
        STATUS: 'FALHA',
        TOTAL_REGISTROS: 0,
        REGISTROS_INSERIDOS: 0,
        REGISTROS_ATUALIZADOS: 0,
        REGISTROS_DELETADOS: 0,
        DURACAO_MS: new Date().getTime() - dataInicioGeral.getTime(),
        MENSAGEM_ERRO: `Erro geral ao sincronizar todas as empresas: ${error.message}`,
        DATA_INICIO: dataInicioGeral,
        DATA_FIM: new Date()
      });
    } catch (logError) {
      console.error('❌ Erro ao salvar log geral de erro:', logError);
    }
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
        FROM AS_VENDEDORES
        WHERE ID_SISTEMA = :idSistema
        GROUP BY ID_SISTEMA`
      : `SELECT
          ID_SISTEMA,
          COUNT(*) as TOTAL_REGISTROS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'S' THEN 1 ELSE 0 END) as REGISTROS_ATIVOS,
          SUM(CASE WHEN SANKHYA_ATUAL = 'N' THEN 1 ELSE 0 END) as REGISTROS_DELETADOS,
          MAX(DT_ULT_CARGA) as ULTIMA_SINCRONIZACAO
        FROM AS_VENDEDORES
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