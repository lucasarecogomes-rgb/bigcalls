# BIGCALLS

BIGCALLS e um analisador pessoal de memecoins da Solana, executado localmente. O objetivo do MVP e reduzir o trabalho manual de pesquisa: coletar dados, eliminar lixo evidente, organizar contexto de mercado/social/on-chain e entregar esse contexto para uma IA interpretar.

Nao e um bot de execucao. O projeto nao possui wallet, buy/sell, gerenciamento de posicoes, copy trade, painel comercial, assinaturas, multiusuario ou qualquer fluxo que movimente fundos.

## Regra de arquitetura

O codigo faz o trabalho deterministico: coleta, normalizacao, historico, metricas e filtros basicos de seguranca. A IA faz o trabalho contextual: entender mercado, narrativa, tendencia, atividade social, influencia e decidir o que merece atencao.

O filtro deterministico deve ser pequeno. Ele existe para descartar lixo/rug obvio, nao para substituir a IA por uma tabela de pontos.

## Fluxo-alvo do MVP

1. Descobrir tokens novos/ativos na Solana.
2. Coletar primeiro o contexto de mercado: market cap, liquidez, volume, idade e atividade.
3. Coletar sinais on-chain basicos: holders, concentracao, creator, snipers/bundles e authorities quando disponivel.
4. Correlacionar sinais sociais e narrativas. O J7 Bridge continua como fonte inicial; outras fontes podem entrar depois.
5. Aplicar apenas o pre-filtro de risco obvio.
6. Enviar o pacote consolidado para a IA.
7. Classificar como `IGNORE`, `OBSERVE` ou `RESEARCH`, com tese, riscos, dados faltantes e proximas verificacoes.
8. Salvar historico local para permitir memoria e comparacao temporal nas proximas fases.

## Estrutura atual

```text
crates/
  app/             API/orquestracao local
  analyst-core/    tipos, pre-filtro, cliente LLM e historico JSONL
j7-bridge/
  extension/       captura sinais do J7Tracker no navegador
  bridge-server/   relay local opcional
```

Todo o codigo antigo de wallet, execucao, posicoes, Telegram, admin, assinaturas, Jupiter, Helius sender e infraestrutura multiusuario foi removido desta base.

## Endpoints atuais

`GET /health`

Retorna o estado basico do processo local.

`POST /webhooks/social/j7`

Recebe e salva eventos do J7. O evento social isolado nao gera uma decisao de investimento; ele vira contexto para ser correlacionado com o token.

`POST /analyze`

Recebe um `market` obrigatorio e uma lista opcional de eventos `social`. O mercado passa primeiro pelo filtro basico e, se nao for rejeitado, o pacote consolidado vai para a IA.

Exemplo minimo:

```json
{
  "market": {
    "contractAddress": "TOKEN_MINT",
    "symbol": "ABC",
    "marketCapUsd": 250000,
    "liquidityUsd": 42000,
    "volume5mUsd": 18000,
    "holders": 850,
    "top10HolderPct": 31.2,
    "creatorHolderPct": 2.1,
    "sniperHolderPct": 8.4
  },
  "social": []
}
```

## Configuracao

Copie `.env.example` para `.env` e configure a chave/modelo de IA quando quiser ativar a camada LLM.

```bash
cargo run -p app
```

Historicos locais sao gravados em `data/` e nao entram no Git.

## Proximas camadas

A base ainda precisa dos adapters que vao alimentar automaticamente o `MarketSnapshot`: descoberta de tokens, dados de mercado e enriquecimento on-chain. Depois entra a correlacao automatica entre o historico social, narrativas em tendencia e os tokens descobertos.

A prioridade permanece: mercado primeiro, depois narrativa/social, depois aprofundamento on-chain e memoria temporal.
