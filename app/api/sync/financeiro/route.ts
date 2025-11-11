
import { NextResponse } from 'next/server';
import { sincronizarFinanceiroPorEmpresa, sincronizarTodasEmpresas, obterEstatisticasSincronizacao } from '@/lib/sync-financeiro-service';

export async function POST(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');
    const empresa = searchParams.get('empresa');

    if (idSistema && empresa) {
      const resultado = await sincronizarFinanceiroPorEmpresa(parseInt(idSistema), empresa);
      return NextResponse.json(resultado);
    } else {
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar financeiro' },
      { status: 500 }
    );
  }
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');

    const estatisticas = await obterEstatisticasSincronizacao(
      idSistema ? parseInt(idSistema) : undefined
    );

    return NextResponse.json(estatisticas);
  } catch (error: any) {
    console.error('❌ Erro ao obter estatísticas:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao obter estatísticas' },
      { status: 500 }
    );
  }
}

export const dynamic = 'force-dynamic';
