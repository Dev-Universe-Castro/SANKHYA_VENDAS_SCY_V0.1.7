
import { NextResponse } from 'next/server';
import {
  sincronizarCabecalhoNotaPorEmpresa,
  sincronizarTodasEmpresas,
  obterEstatisticasSincronizacao
} from '@/lib/sync-cabecalho-nota-service';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistemaParam = searchParams.get('idSistema');

    const idSistema = idSistemaParam ? parseInt(idSistemaParam) : undefined;
    const estatisticas = await obterEstatisticasSincronizacao(idSistema);

    return NextResponse.json(estatisticas);
  } catch (error: any) {
    console.error('Erro ao obter estatísticas de sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao obter estatísticas' },
      { status: 500 }
    );
  }
}

export async function POST(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistemaParam = searchParams.get('idSistema');
    const empresaParam = searchParams.get('empresa');

    if (idSistemaParam && empresaParam) {
      const idSistema = parseInt(idSistemaParam);
      const resultado = await sincronizarCabecalhoNotaPorEmpresa(idSistema, empresaParam);
      return NextResponse.json(resultado);
    } else {
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('Erro ao sincronizar cabeçalhos de nota:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar cabeçalhos de nota' },
      { status: 500 }
    );
  }
}
