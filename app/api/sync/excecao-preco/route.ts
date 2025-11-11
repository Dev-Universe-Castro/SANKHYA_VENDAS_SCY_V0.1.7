
import { NextResponse } from 'next/server';
import { sincronizarExcecaoPrecoPorEmpresa, sincronizarTodasEmpresas, obterEstatisticasSincronizacao } from '@/lib/sync-excecao-preco-service';

export async function POST(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const idSistema = searchParams.get('idSistema');
    const empresa = searchParams.get('empresa');

    if (idSistema && empresa) {
      // Sincronizar empresa específica
      const resultado = await sincronizarExcecaoPrecoPorEmpresa(parseInt(idSistema), empresa);
      return NextResponse.json(resultado);
    } else {
      // Sincronizar todas as empresas (uma por vez)
      const resultados = await sincronizarTodasEmpresas();
      return NextResponse.json(resultados);
    }
  } catch (error: any) {
    console.error('❌ Erro na sincronização:', error);
    return NextResponse.json(
      { error: error.message || 'Erro ao sincronizar exceções de preço' },
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
