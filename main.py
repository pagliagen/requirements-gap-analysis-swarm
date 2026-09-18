"""
Agente di gap-analysis dei requisiti per integrazioni HubSpot custom.

Dato un brief grezzo (appunti da call o email col cliente), un orchestratore
progetta una squadra di agenti specializzati su misura per quel brief, ognuno
analizza il brief nella propria area, e un coordinatore finale unisce e
priorizza le domande aperte.

Uso:
    python main.py --brief brief.txt
    cat brief.txt | python main.py --brief -
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime

import anthropic
import httpx2
from dotenv import load_dotenv

# Carica ANTHROPIC_API_KEY e ANTHROPIC_MODEL da .env se presente; le variabili
# già esportate nella shell hanno la precedenza.
load_dotenv()

# ---------------------------------------------------------------------------
# Configurazione
# ---------------------------------------------------------------------------

MODELLO = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5")

# Limiti hard-coded: 1 orchestratore + MAX_AGENTI agenti + 1 coordinatore <= MAX_CHIAMATE_API
MAX_CHIAMATE_API = 8
MAX_AGENTI = 6

# Timeout per singola chiamata (secondi): il coordinatore può ragionare a lungo in silenzio.
TIMEOUT_CHIAMATA = 1200.0

# I risultati degli agenti vengono salvati qui, così un errore nel coordinatore
# non brucia le chiamate già fatte (vedi --solo-coordinatore).
FILE_CACHE_AGENTI = ".cache_agenti.json"

# Checklist di copertura: le aree che la squadra di agenti generata deve coprire
# collettivamente. NON sono agenti predefiniti: l'orchestratore progetta agenti
# su misura per il brief e dichiara quali aree ciascuno copre.
AREE_DA_COPRIRE = [
    {
        "id": "volumi_performance",
        "nome": "Volumi e performance",
        "focus": "volume di dati, chiamate API previste, picchi (import iniziale vs incrementale), crescita, rate limit",
    },
    {
        "id": "autenticazione_accessi",
        "nome": "Autenticazione e accessi",
        "focus": "proprietà/admin dell'account HubSpot, autenticazione sul sistema esterno, scope OAuth, sandbox",
    },
    {
        "id": "mapping_dati",
        "nome": "Mapping dati",
        "focus": "campi mappati tra i sistemi, campi mancanti o di tipo diverso, ID univoco comune, oggetti HubSpot coinvolti",
    },
    {
        "id": "gestione_errori_edge_case",
        "nome": "Gestione errori ed edge case",
        "focus": "sistema down, duplicati, retry su webhook falliti, notifiche di errore, record parziali, cancellazioni",
    },
    {
        "id": "direzione_timing_flusso",
        "nome": "Direzione e timing del flusso",
        "focus": "direzione del flusso, realtime vs batch, source of truth nei conflitti se bidirezionale, eventi trigger",
    },
    {
        "id": "requisiti_non_funzionali",
        "nome": "Requisiti non funzionali",
        "focus": "SLA, ambiente di test/staging, manutenzione post go-live, budget e vincoli di tempo, hosting, monitoraggio",
    },
    {
        "id": "compliance_privacy",
        "nome": "Compliance e privacy",
        "focus": "dati personali coinvolti, dove sono ospitati, GDPR e vincoli normativi, base giuridica, retention, DPA",
    },
]

# Etichette esatte di priorità usate dal coordinatore; l'ordine definisce il sort del report.
PRIORITA = ["Bloccante", "Importante", "Da chiarire"]


# ---------------------------------------------------------------------------
# Contatore chiamate API
# ---------------------------------------------------------------------------

class ContatoreChiamate:
    """Tiene il conto delle chiamate API e blocca il superamento del limite."""

    def __init__(self, limite: int):
        self.limite = limite
        self.numero = 0

    def registra(self, etichetta: str) -> None:
        if self.numero >= self.limite:
            raise RuntimeError(
                f"Raggiunto il limite massimo di {self.limite} chiamate API, "
                f"chiamata rifiutata: {etichetta}"
            )
        self.numero += 1
        print(f"[Chiamata API {self.numero}/{self.limite}] {etichetta}")


# ---------------------------------------------------------------------------
# Helper per le chiamate al modello
# ---------------------------------------------------------------------------

def chiama_modello(
    client: anthropic.Anthropic,
    contatore: ContatoreChiamate,
    etichetta: str,
    system: str,
    user: str,
    max_tokens: int,
    effort: str,
) -> dict:
    """Esegue una chiamata al modello e restituisce il JSON contenuto nella risposta."""
    contatore.registra(etichetta)

    # Streaming: max_tokens alti non rischiano timeout HTTP. Il thinking adattivo
    # consuma parte del budget di output, quindi i limiti devono essere generosi.
    try:
        with client.messages.stream(
            model=MODELLO,
            max_tokens=max_tokens,
            system=system,
            output_config={"effort": effort},
            messages=[{"role": "user", "content": user}],
        ) as stream:
            risposta = stream.get_final_message()
    except anthropic.AuthenticationError:
        raise SystemExit("Errore: API key non valida (controlla ANTHROPIC_API_KEY).")
    except anthropic.RateLimitError as e:
        attesa = e.response.headers.get("retry-after", "?")
        raise SystemExit(f"Errore: rate limit raggiunto, riprova tra {attesa}s.")
    except anthropic.BadRequestError as e:
        raise SystemExit(f"Errore nella richiesta al modello: {e.message}")
    except anthropic.APIStatusError as e:
        raise SystemExit(f"Errore API ({e.status_code}): {e.message}")
    except anthropic.APIConnectionError:
        raise SystemExit("Errore di rete: impossibile raggiungere l'API Anthropic.")
    except httpx2.TimeoutException:
        # Lo streaming non incapsula i timeout in APITimeoutError: arrivano grezzi da httpx.
        raise SystemExit(
            f"Errore: timeout dopo {TIMEOUT_CHIAMATA:.0f}s in attesa del modello ({etichetta}). "
            f"Se i risultati degli agenti sono in cache, rilancia con --solo-coordinatore."
        )

    if risposta.stop_reason == "max_tokens":
        raise SystemExit(
            f"Errore: risposta troncata per max_tokens ({etichetta}). "
            f"Aumenta il limite nella chiamata o riduci il brief."
        )

    testo = "".join(b.text for b in risposta.content if b.type == "text")
    return estrai_json(testo)


def estrai_json(testo: str) -> dict:
    """Estrae il primo oggetto JSON dal testo, tollerando eventuali code fence."""
    match = re.search(r"\{.*\}", testo, re.DOTALL)
    if not match:
        raise ValueError(f"Nessun JSON trovato nella risposta del modello:\n{testo[:500]}")
    return json.loads(match.group(0))


# ---------------------------------------------------------------------------
# Fase 1: orchestratore - progetta la squadra di agenti
# ---------------------------------------------------------------------------

def orchestratore(client, contatore, brief: str) -> tuple[list[dict], list[dict]]:
    """Progetta gli agenti specializzati su misura per il brief.

    Restituisce (agenti, aree_non_coperte). Ogni agente ha nome, ruolo, focus,
    istruzioni e l'elenco delle aree della checklist che copre.
    """
    tetto = min(MAX_AGENTI, MAX_CHIAMATE_API - 2)
    checklist = "\n".join(f"- id: {a['id']} | {a['nome']}: {a['focus']}" for a in AREE_DA_COPRIRE)

    system = f"""Sei il lead tecnico di una software house che realizza integrazioni custom tra
HubSpot e sistemi esterni. Riceverai gli appunti grezzi di un brief cliente. Il tuo compito
NON è analizzare il brief, ma PROGETTARE la squadra di analisti (agenti AI) che lo
analizzerà per trovare cosa manca prima di scrivere la spec tecnica.

Progetta al massimo {tetto} agenti. Ogni agente deve essere specializzato su un aspetto
concreto di QUESTO progetto, non su una categoria astratta: se il brief parla di un
gestionale specifico, di un tipo di dato particolare, di un vincolo organizzativo, crea
agenti che sappiano di quelle cose. Pensa a chi vorresti davvero in una riunione di
kickoff su questo progetto.

Collettivamente la squadra deve coprire questa checklist di aree dei requisiti:
{checklist}

Regole:
- Un agente può coprire più aree della checklist, un'area può essere coperta da più agenti.
- Un'area va lasciata scoperta solo se è chiaramente non applicabile alla natura del
  progetto: l'assenza di informazioni nel brief NON la rende non applicabile, la rende
  un vuoto che l'agente competente dovrà segnalare.
- "Compliance e privacy" va sempre coperta se il progetto tratta dati di persone fisiche
  o dati finanziari.
- Per ogni area non coperta spiega il motivo.
- Prima di rispondere, rileggi il brief elemento per elemento: ogni sistema, oggetto di
  business (es. listini, prodotti, ordini, fatture), numero, persona, scadenza o vincolo
  citato deve comparire esplicitamente nel focus di almeno un agente. Se qualcosa non è
  assegnato a nessuno, nessuno lo indagherà.

Per ogni agente fornisci:
- nome: breve, descrittivo (es. "Esperto connettori gestionali on-premise")
- etichetta: 1-3 parole per tabelle e titoli (es. "Connettore Easyfatt"), unica nella squadra
- ruolo: chi è, in una o due frasi, con le competenze rilevanti per questo brief
- focus: cosa deve cercare specificamente in questo brief, con riferimenti concreti al
  suo contenuto (nomi di sistemi, numeri, persone, vincoli citati)
- istruzioni: eventuali indicazioni particolari su cosa approfondire o su trappole
  tipiche di questo tipo di progetto che deve verificare
- aree_coperte: lista di id della checklist

Rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:
{{
  "agenti": [
    {{"nome": "...", "etichetta": "...", "ruolo": "...", "focus": "...", "istruzioni": "...", "aree_coperte": ["id", "id"]}}
  ],
  "aree_non_coperte": [{{"id": "...", "motivo": "..."}}]
}}"""

    user = f"BRIEF DEL CLIENTE (appunti grezzi):\n\n{brief}"

    risultato = chiama_modello(
        client, contatore,
        etichetta="Orchestratore: progettazione della squadra di agenti",
        system=system, user=user, max_tokens=16000, effort="high",
    )

    ids_validi = {a["id"] for a in AREE_DA_COPRIRE}
    agenti = []
    etichette_usate = set()
    for voce in risultato.get("agenti", []):
        if not voce.get("nome") or not voce.get("focus"):
            continue
        nome = voce["nome"].strip()
        # L'etichetta identifica l'agente in tabelle e titoli: deve essere unica.
        etichetta = (voce.get("etichetta") or nome).strip()
        if etichetta in etichette_usate:
            etichetta = nome
        etichette_usate.add(etichetta)
        agenti.append({
            "nome": nome,
            "etichetta": etichetta,
            "ruolo": voce.get("ruolo", ""),
            "focus": voce["focus"],
            "istruzioni": voce.get("istruzioni", ""),
            "aree_coperte": [a for a in voce.get("aree_coperte", []) if a in ids_validi],
        })

    if not agenti:
        raise SystemExit("L'orchestratore non ha progettato nessun agente valido.")

    # Il tetto sugli agenti e quello sulle chiamate devono restare coerenti:
    # orchestratore + agenti + coordinatore <= MAX_CHIAMATE_API.
    tagliati = agenti[tetto:]
    agenti = agenti[:tetto]

    # Copertura effettiva della checklist, calcolata dal codice e non fidandosi
    # della dichiarazione del modello.
    coperte = {a for ag in agenti for a in ag["aree_coperte"]}
    motivi = {v.get("id"): v.get("motivo", "") for v in risultato.get("aree_non_coperte", [])}
    non_coperte = []
    for area in AREE_DA_COPRIRE:
        if area["id"] in coperte:
            continue
        if any(area["id"] in t["aree_coperte"] for t in tagliati):
            motivo = f"coperta solo da un agente oltre il tetto di {tetto}"
        else:
            motivo = motivi.get(area["id"]) or "non assegnata a nessun agente dall'orchestratore"
        non_coperte.append({"nome": area["nome"], "motivo": motivo})

    return agenti, non_coperte


# ---------------------------------------------------------------------------
# Fase 2: esecuzione generica di un agente progettato dall'orchestratore
# ---------------------------------------------------------------------------

def esegui_agente(client, contatore, brief: str, agente: dict, squadra: list[dict]) -> dict:
    """Esegue un agente generato: analizza il brief nel suo perimetro e restituisce il JSON."""
    nomi_aree = [a["nome"] for a in AREE_DA_COPRIRE if a["id"] in agente["aree_coperte"]]
    colleghi = "\n".join(
        f"- {c['nome']}: {c['focus']}" for c in squadra if c["nome"] != agente["nome"]
    )

    system = f"""{agente['ruolo']}

Fai parte di una squadra di analisti che esamina il brief grezzo di un cliente per
un'integrazione HubSpot custom. Il tuo perimetro è questo, e solo questo:

FOCUS: {agente['focus']}

{"ISTRUZIONI PARTICOLARI: " + agente['istruzioni'] if agente['istruzioni'] else ""}

Aree dei requisiti che copri: {", ".join(nomi_aree) or "quelle indicate nel focus"}.

I tuoi colleghi, con il rispettivo perimetro:
{colleghi or "- (nessuno)"}

Non duplicare il loro lavoro: se un'informazione o una domanda ricade nel perimetro di un
collega, lasciala a lui anche se la conosci. Riportala solo se è indispensabile al tuo
ragionamento, e in quel caso in una sola riga.

Produci tre liste:
1. informazioni_presenti: cosa il brief dice già nel tuo perimetro, una riga sintetica
   per ogni informazione. Non inventare nulla che non sia nel brief.
2. domande_aperte: cosa manca per scrivere una spec tecnica solida. Ogni domanda deve
   essere specifica, diretta e pronta da copiare in un'email al cliente.
   - NO: "Chiarire i volumi."
   - SÌ: "Quante chiamate al minuto prevedete nei picchi dell'import iniziale?"
   Evita domande la cui risposta è già nel brief.
3. vincoli_noti: al massimo 2 fatti tecnici o di licenza che sai con certezza e che
   impattano il tuo perimetro (es. una funzionalità assente nel piano HubSpot indicato).
   Solo fatti verificabili, mai valutazioni di rischio o considerazioni generiche.
   Se non sei certo, lista vuota: un vincolo sbagliato è peggio di nessun vincolo.

Se il brief non tocca minimamente il tuo perimetro, imposta area_scoperta a true, lascia
informazioni_presenti vuota e formula comunque le domande necessarie a coprirlo.

Rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:
{{
  "area_scoperta": true|false,
  "informazioni_presenti": ["...", "..."],
  "domande_aperte": ["...", "..."],
  "vincoli_noti": ["..."]
}}"""

    user = f"BRIEF DEL CLIENTE (appunti grezzi):\n\n{brief}"

    risultato = chiama_modello(
        client, contatore,
        etichetta=f"Agente '{agente['nome']}'",
        system=system, user=user, max_tokens=16000, effort="medium",
    )
    return {
        "area": agente["etichetta"],
        "area_scoperta": bool(risultato.get("area_scoperta", False)),
        "informazioni_presenti": risultato.get("informazioni_presenti", []),
        "domande_aperte": risultato.get("domande_aperte", []),
        "vincoli_noti": risultato.get("vincoli_noti", [])[:2],
    }


# ---------------------------------------------------------------------------
# Fase 3: coordinatore
# ---------------------------------------------------------------------------

def coordinatore(client, contatore, brief: str, risultati: list[dict]) -> dict:
    """Unisce, deduplica e priorizza le domande degli agenti; produce la bozza email."""
    nomi_agenti = "\n".join(f"- {r['area']}" for r in risultati)

    system = f"""Sei il coordinatore di un team di analisti che ha esaminato il brief di un cliente
per un'integrazione HubSpot custom. Ogni analista ha coperto un perimetro e ha prodotto
informazioni presenti, domande aperte ed eventuali vincoli noti. Il tuo compito:

1. Unire tutte le domande aperte, eliminando duplicati e sovrapposizioni (se due analisti
   fanno la stessa domanda con parole diverse, tienine una sola, attribuita all'analista
   più pertinente). Puoi riformulare leggermente per chiarezza, mantenendo le domande
   dirette e pronte da inviare al cliente.
   Il campo "area" di ogni domanda deve essere ESATTAMENTE uno di questi nomi, senza
   modifiche né accorpamenti:
{nomi_agenti}
2. Assegnare a ogni domanda una priorità, usando ESATTAMENTE una di queste etichette:
   - "{PRIORITA[0]}": senza risposta non si può scrivere la spec tecnica. Tipicamente:
     chiave di matching tra i sistemi, source of truth nei flussi bidirezionali, come
     autenticarsi e raggiungere il sistema esterno, quali dati compongono gli oggetti
     da creare, decisioni architetturali obbligate.
   - "{PRIORITA[1]}": influenza scelte tecniche o di progetto importanti. Tipicamente:
     realtime vs batch, gestione errori e duplicati, sandbox, hosting, scadenze, budget,
     manutenzione, dove risiedono i dati personali e chi ne è responsabile (DPA).
     Scadenze e budget non sono mai Bloccanti: condizionano la spec ma non impediscono
     di scriverla.
   - "{PRIORITA[2]}": utile ma non blocca la stesura della spec.
   Tieni conto dei vincoli_noti: se un vincolo rende una domanda inutile o ne cambia il
   senso, riformulala o scartala.
3. Stimare qualitativamente la completezza del brief: un livello (Bassa / Media / Alta),
   una fascia approssimativa (es. "circa 30-40%", mai un numero preciso e finto) e una
   motivazione di due o tre frasi.
4. Scrivere una bozza di email in italiano al cliente, tono professionale e cordiale,
   che ringrazi per il brief e chieda le risposte alle sole domande Bloccanti e Importanti,
   raggruppate per tema, con placeholder [Nome] per i nomi. Niente domande "Da chiarire".

Rispondi ESCLUSIVAMENTE con un oggetto JSON valido, senza testo prima o dopo:
{{
  "completezza": {{"livello": "Bassa|Media|Alta", "fascia": "...", "motivazione": "..."}},
  "domande": [{{"area": "...", "domanda": "...", "priorita": "{PRIORITA[0]}|{PRIORITA[1]}|{PRIORITA[2]}"}}],
  "email": {{"oggetto": "...", "corpo": "..."}}
}}"""

    user = (
        f"BRIEF ORIGINALE DEL CLIENTE:\n\n{brief}\n\n"
        f"---\n\nRISULTATI DEGLI ANALISTI (JSON):\n\n{json.dumps(risultati, ensure_ascii=False, indent=2)}"
    )

    risultato = chiama_modello(
        client, contatore,
        etichetta="Coordinatore: unione, priorità e bozza email",
        system=system, user=user, max_tokens=32000, effort="medium",
    )
    risultato.setdefault("completezza", {})
    risultato.setdefault("domande", [])
    risultato.setdefault("email", {})
    return risultato


# ---------------------------------------------------------------------------
# Report Markdown
# ---------------------------------------------------------------------------

def genera_report(
    brief_path: str,
    agenti: list[dict],
    non_coperte: list[dict],
    risultati: list[dict],
    finale: dict,
) -> str:
    """Compone il report Markdown a partire dagli output strutturati degli agenti."""
    per_id = {a["id"]: a["nome"] for a in AREE_DA_COPRIRE}
    righe = []
    righe.append("# Gap analysis dei requisiti")
    righe.append("")
    righe.append(f"_Brief: `{brief_path}` — generato il {datetime.now():%d/%m/%Y %H:%M}_")
    righe.append("")

    # 1. Sommario
    comp = finale["completezza"]
    righe.append("## 1. Sommario")
    righe.append("")
    righe.append(
        f"**Completezza stimata del brief: {comp.get('livello', 'n/d')}** "
        f"({comp.get('fascia', 'n/d')})"
    )
    righe.append("")
    righe.append(comp.get("motivazione", ""))
    righe.append("")

    scoperte = [r["area"] for r in risultati if r["area_scoperta"]]
    if scoperte:
        righe.append("**Perimetri completamente scoperti nel brief:** " + ", ".join(scoperte))
        righe.append("")

    righe.append("**Squadra di agenti generata per questo brief:**")
    for a in agenti:
        aree = ", ".join(per_id[i] for i in a["aree_coperte"]) or "nessuna area della checklist"
        righe.append(f"- **{a['etichetta']}** · {a['nome']} — {a['ruolo']} _(copre: {aree})_")
    righe.append("")

    if non_coperte:
        righe.append("**Aree della checklist non coperte:**")
        righe.extend(f"- {n['nome']}: {n['motivo']}" for n in non_coperte)
        righe.append("")

    vincoli = [(r["area"], v) for r in risultati for v in r["vincoli_noti"]]
    if vincoli:
        righe.append("**Vincoli noti da tenere presenti** (segnalati dagli agenti, non dal cliente; da verificare):")
        righe.extend(f"- _{area}_: {v}" for area, v in vincoli)
        righe.append("")

    # 2. Tabella domande aperte, ordinata per priorità
    righe.append("## 2. Domande aperte")
    righe.append("")
    righe.append("| Priorità | Area | Domanda |")
    righe.append("|---|---|---|")
    ordine = {p: i for i, p in enumerate(PRIORITA)}
    domande = sorted(finale["domande"], key=lambda d: ordine.get(d.get("priorita"), len(PRIORITA)))
    for d in domande:
        domanda = d.get("domanda", "").replace("|", "\\|").replace("\n", " ")
        righe.append(f"| {d.get('priorita', 'n/d')} | {d.get('area', 'n/d')} | {domanda} |")
    righe.append("")

    # 3. Cosa sappiamo già, direttamente dalle righe degli agenti
    righe.append("## 3. Cosa sappiamo già")
    righe.append("")
    for r in risultati:
        righe.append(f"### {r['area']}")
        righe.append("")
        if r["area_scoperta"] or not r["informazioni_presenti"]:
            righe.append("_Perimetro scoperto: il brief non fornisce informazioni._")
        else:
            righe.extend(f"- {info}" for info in r["informazioni_presenti"])
        righe.append("")

    # 4. Bozza email
    email = finale["email"]
    righe.append("## 4. Bozza email per il cliente")
    righe.append("")
    righe.append(f"**Oggetto:** {email.get('oggetto', '')}")
    righe.append("")
    righe.append(email.get("corpo", ""))
    righe.append("")

    return "\n".join(righe)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def leggi_brief(percorso: str) -> str:
    """Legge il brief da file, oppure da stdin se il percorso è '-'."""
    if percorso == "-":
        testo = sys.stdin.read()
    else:
        with open(percorso, encoding="utf-8") as f:
            testo = f.read()
    if not testo.strip():
        raise SystemExit("Errore: il brief è vuoto.")
    return testo.strip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gap analysis dei requisiti per integrazioni HubSpot custom."
    )
    parser.add_argument("--brief", required=True, help="File .txt con il brief, oppure '-' per stdin")
    parser.add_argument("--output", default="report_gap_analysis.md", help="File Markdown di output")
    parser.add_argument(
        "--solo-coordinatore", action="store_true",
        help=f"Salta orchestratore e agenti, riusa i risultati salvati in {FILE_CACHE_AGENTI}",
    )
    args = parser.parse_args()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise SystemExit(
            "Errore: ANTHROPIC_API_KEY non impostata. Esportala nella shell oppure "
            "copia .env.example in .env e inserisci la chiave."
        )

    brief = leggi_brief(args.brief)
    client = anthropic.Anthropic(timeout=TIMEOUT_CHIAMATA)
    contatore = ContatoreChiamate(MAX_CHIAMATE_API)

    print(f"Brief caricato ({len(brief)} caratteri). Modello: {MODELLO}\n")

    if args.solo_coordinatore:
        if not os.path.exists(FILE_CACHE_AGENTI):
            raise SystemExit(f"Errore: nessuna cache trovata in {FILE_CACHE_AGENTI}.")
        with open(FILE_CACHE_AGENTI, encoding="utf-8") as f:
            cache = json.load(f)
        agenti, non_coperte, risultati = cache["agenti"], cache["non_coperte"], cache["risultati"]
        print(f"  Riuso dei risultati di {len(risultati)} agenti da {FILE_CACHE_AGENTI}\n")
    else:
        agenti, non_coperte = orchestratore(client, contatore, brief)
        print(f"  Squadra generata ({len(agenti)}/{MAX_AGENTI} agenti):")
        for a in agenti:
            print(f"    - {a['nome']}: {a['focus']}")
        for n in non_coperte:
            print(f"    - [area non coperta] {n['nome']}: {n['motivo']}")
        print()

        risultati = [esegui_agente(client, contatore, brief, a, agenti) for a in agenti]
        for r in risultati:
            stato = "PERIMETRO SCOPERTO" if r["area_scoperta"] else f"{len(r['informazioni_presenti'])} info"
            print(f"    {r['area']}: {stato}, {len(r['domande_aperte'])} domande, {len(r['vincoli_noti'])} vincoli")
        print()

        with open(FILE_CACHE_AGENTI, "w", encoding="utf-8") as f:
            json.dump(
                {"agenti": agenti, "non_coperte": non_coperte, "risultati": risultati},
                f, ensure_ascii=False, indent=2,
            )

    finale = coordinatore(client, contatore, brief, risultati)

    report = genera_report(args.brief, agenti, non_coperte, risultati, finale)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\nChiamate API totali: {contatore.numero}/{MAX_CHIAMATE_API}")
    print(f"Report salvato in: {args.output}\n")
    print(report)


if __name__ == "__main__":
    main()
