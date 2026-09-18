# Requirements Gap Analysis Swarm

Agente multi-agente di gap-analysis dei requisiti per integrazioni HubSpot custom.

Dato un brief grezzo (appunti da una call o un'email col cliente), lo script:

1. **Orchestratore** — legge il brief e seleziona le dimensioni dei requisiti pertinenti
   (volumi, autenticazione, mapping dati, errori, direzione del flusso, requisiti non
   funzionali, compliance), al massimo 6.
2. **Agenti specializzati** — uno per dimensione, ognuno elenca cosa il brief dice già
   nella sua area e formula domande aperte pronte da inviare al cliente. Se un'area è
   del tutto assente dal brief, la segnala come scoperta.
3. **Coordinatore** — unisce e deduplica le domande, le ordina per priorità
   (Bloccante / Importante / Da chiarire), stima la completezza del brief e scrive la
   bozza email per il cliente.

L'output è un report Markdown. Non genera codice: serve come consulente prima di
scrivere la spec tecnica.

## Requisiti

- Python 3.10+
- Una API key Anthropic

## Installazione

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Apri `.env` e inserisci la tua chiave e il modello:

```
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-5
```

In alternativa esporta la variabile nella shell (`export ANTHROPIC_API_KEY=...`):
ha la precedenza sul file `.env`.

Se preferisci attivare il virtualenv invece di usare il prefisso `.venv/bin/`:

```bash
source .venv/bin/activate   # con `source`, non eseguendolo direttamente
python main.py --brief brief_esempio.txt
```

## Uso

```bash
# da file
.venv/bin/python main.py --brief brief_esempio.txt

# incollando il testo da stdin (termina con Ctrl-D)
.venv/bin/python main.py --brief -

# salvando il report con un nome diverso
.venv/bin/python main.py --brief brief.txt --output analisi_rossi.md
```

Il brief può essere testo libero e disordinato: non serve strutturarlo.

Durante l'esecuzione la console mostra il contatore delle chiamate API
(`[Chiamata API 3/8] Agente 'Mapping dati'`) e le dimensioni selezionate. Il report
viene salvato in `report_gap_analysis.md` (o nel file indicato con `--output`) e
stampato a console.

## Struttura del report

1. **Sommario** — livello di completezza del brief (Bassa / Media / Alta, con fascia
   approssimativa), aree scoperte, aree analizzate ed escluse
2. **Domande aperte** — tabella ordinata per priorità con Area e Domanda
3. **Cosa sappiamo già** — per area, una riga per ogni informazione presente nel brief
4. **Bozza email** — pronta da inviare al cliente, con le sole domande bloccanti e
   importanti

## Configurazione

Le costanti in cima a `main.py`:

| Costante | Default | Significato |
|---|---|---|
| `MODELLO` | `ANTHROPIC_MODEL` o `claude-sonnet-5` | Modello usato per tutte le chiamate, letto dall'ambiente / `.env` |
| `MAX_CHIAMATE_API` | `8` | Tetto hard sulle chiamate API per esecuzione |
| `MAX_AGENTI` | `6` | Tetto sul numero di agenti specializzati generati |

Il numero di chiamate è sempre `1 + agenti + 1`, quindi con i default il caso peggiore
usa esattamente 8 chiamate. Se alzi `MAX_AGENTI`, gli agenti vengono comunque tagliati a
`MAX_CHIAMATE_API - 2`.

Le dimensioni analizzabili sono definite nella lista `DIMENSIONI`: per aggiungerne una
basta una nuova voce con `id`, `nome` e `focus`.

## File

- `main.py` — lo script
- `brief_esempio.txt` — un brief grezzo di esempio per provare il tool
- `.env.example` — modello per la configurazione della chiave
- `requirements.txt` — dipendenze (`anthropic`, `python-dotenv`)
# requirements-gap-analysis-swarm
