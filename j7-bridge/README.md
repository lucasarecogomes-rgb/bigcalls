# BIGCALLS J7 Bridge

Coletor local usado pelo BIGCALLS para capturar sinais exibidos no J7Tracker e envia-los ao backend local.

## Componentes

- `extension/`: extensao Chrome/Edge que observa o J7Tracker e normaliza eventos.
- `bridge-server/`: relay HTTP local opcional.

O payload inclui, quando disponivel: autor, username, texto, contract address, ticker, nome do token, links e timestamp.

O endpoint padrao do backend BIGCALLS e:

```text
http://127.0.0.1:8790/webhooks/social/j7
```

O J7 e apenas uma fonte social. Ele nao substitui descoberta de tokens, dados de mercado ou analise on-chain.

## Extensao

```bash
cd j7-bridge/extension
npm install
npm run check
npm run build
```

O build gera `dist/content.js`. A pasta `dist/` e gerada localmente e nao fica versionada.

Depois carregue `j7-bridge/extension` como extensao descompactada no Chrome/Edge.

## Relay opcional

```bash
cd j7-bridge/bridge-server
npm start
```

Por padrao ele escuta em `127.0.0.1:8791` e encaminha para o backend BIGCALLS em `127.0.0.1:8790`.

Variaveis opcionais:

```env
J7_BRIDGE_HOST=127.0.0.1
J7_BRIDGE_PORT=8791
BIGCALLS_FORWARD_URL=http://127.0.0.1:8790/webhooks/social/j7
BIGCALLS_AUTH_TOKEN=
```
