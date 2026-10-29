# How the Project Works: Simple Workflow

This explains the project step by step in plain words: what happens at each step, and **why** that
step is needed. After the steps there's a worked **example** that follows one attack all the way from
the CSV file to the dashboard. Difficult terms are explained in the glossary at the end.

---

## The big picture

We want to **spot network attacks the moment they happen**.

Think of it like a security guard watching a building's entrance:
- network **flows** (connections between computers) are the visitors,
- **Kafka** is the waiting line at the door,
- **Spark** is the guard checking each visitor,
- the **model** is the guard's experience of what suspicious people look like,
- the **dashboard** is the screen in the security room.

```
 CSV files ──► Producer ──► Kafka ──► Spark ──► Bronze ──► Silver ──► Gold
 (recorded     (sends       (waiting   (does the  (raw copy) (cleaned)  (summaries
  traffic)      flows)       line)      work)                            + alerts)
                                                              │
                                                    Model is trained here
                                                              │
                                                              ▼
                                                         Dashboard
```

---

## Step 0: Start the computers (Docker)

**What happens:** `make cluster-up` starts four small "virtual computers" (containers) on the laptop:
one for Kafka, one for the Kafka UI web page, a Spark master and a Spark worker.

**Why Docker is needed:**
- Kafka and Spark are big programs with many dependencies (Java, libraries, settings). Installing
  them by hand is slow and error-prone.
- Docker packages each one in a ready-made box. One command starts all of them, and they work the
  same way on any machine.
- It also lets one laptop act like a small **cluster** (several computers working together), which
  is how big data systems run in real life.

---

## Step 1: The data

**What happens:** we use **CICIDS2017**, a famous dataset of real network traffic recorded over 5
days: 2.8 million flows. Each flow is labelled as *normal* or as an *attack* (DoS, port scan, brute
force, and so on).

**Why it's needed:** we don't have a real company network to watch, so we use a recorded one.
Because every flow has a correct label, we can also check whether our system catches the attacks.

**Simplification for the demo:** we keep all attack flows but only 20% of the normal ones (about 1
million flows), so the laptop runs fast.

---

## Step 2: The producer (pretends to be a live network)

**What happens:** a small program (`make backfill` / `make live`) reads the files and **sends the
flows one by one into Kafka**, at a controlled speed, as if they were happening right now.

**Why it's needed:** real systems receive data continuously, not as one big file. The producer
recreates that "live feed" so the rest of the system works exactly as it would in real life.

It sends the data in **two rounds**:
1. **Backfill (80% of flows):** the past history, used to **train** the model.
2. **Live (20%, kept hidden from training):** "new" traffic that the trained model must judge. This
   is what you show in the demo.

---

## Step 3: Kafka (the waiting line)

**What happens:** every flow is stored in Kafka, in a **topic** called `flows.raw`, until Spark reads it.

**Why Kafka is needed:**
- **It's a buffer.** If data arrives faster than Spark can process it, or Spark restarts, nothing is
  lost. The data simply waits in line.
- **It keeps a log.** Kafka remembers the messages, so we can replay them later.
- **It splits the work.** The topic is divided into 12 **partitions** (12 smaller lines), so several
  workers can read in parallel.
- **It's the only way in.** All data enters through Kafka. This one-door design is called a
  **Kappa architecture**.

**Why Kafka runs in Docker:** Kafka is a server that must run all the time, with its own settings,
ports and storage. Docker starts it in seconds, already configured, and keeps its data in a Docker
volume.

---

## Step 4: Spark Structured Streaming (does the work)

**What happens:** `make stream` starts one Spark program that runs all the time. Every 5 seconds it
picks up the new flows from Kafka and pushes them through the steps below.

**Why Spark is needed:**
- Millions of rows are too many for a normal Python script to handle quickly. Spark splits the work
  into small tasks and runs them in parallel across the cluster.
- **Structured Streaming** means it processes data **continuously, in small batches**, instead of
  waiting for all the data to arrive.
- It remembers how far it got (**checkpoints**). If it crashes, it continues from the same spot,
  with no data lost and none processed twice.

### The 5 queries that `make stream` starts

A **query** is one continuous job inside the Spark program. When `make stream` starts, it prints
these 5 names, and you'll see the same names on the dashboard's **Pipeline** tab:

| Query name | What it does | Why it's needed | Explained in |
|---|---|---|---|
| `q1_bronze` | Reads new flows from Kafka and saves an exact raw copy | Keeps the original data safe, so it can be re-processed later | Step 5 |
| `q2_silver` | Cleans and checks each flow; sends broken ones to quarantine | Everything after this needs clean, trustworthy data | Step 6 |
| `q3a_gold_timeline` | Counts flows per minute for each attack type | Answers "what kind of attacks are happening right now?" | Step 7 |
| `q3b_gold_hosts` | Counts each computer's activity per minute; spots port scans | Answers "which computer is attacking?" | Step 7 |
| `q4_scoring` | Runs the trained model on every live flow and raises alerts | This is the actual attack detection | Step 9 |

**How they connect:** each query feeds the next, like an assembly line:

```
Kafka ─► q1_bronze ─► q2_silver ─┬─► q3a_gold_timeline   (attacks per minute)
                                 ├─► q3b_gold_hosts      (per-computer activity, port scans)
                                 └─► q4_scoring          (model → alerts)
```

**Why 5 separate queries instead of one big one:**
- **Each has one simple job,** so it's easy to understand, test and fix.
- **They run at the same time,** sharing the cluster's two CPU cores.
- **Each keeps its own checkpoint (bookmark).** If one is slow or restarts, the others aren't affected.
- **Each saves its result as a table,** so every stage can be inspected on its own.

The rest of this guide follows the same order: bronze → silver → gold → scoring.

---

## Step 5: Bronze (keep an exact raw copy)

**What happens:** every flow is saved exactly as it arrived, without any changes.

**Why it's needed:** it's a backup of the original data. If we later find a mistake in our cleaning
logic, we can fix it and re-process from bronze, without asking the source to send everything again.

---

## Step 6: Silver (clean the data)

**What happens:** each flow is cleaned and checked:
- dates and numbers are fixed (some values were broken, like "Infinity"),
- attack labels are tidied into 8 simple groups,
- **broken records are not thrown away.** They're moved to a separate "quarantine" table (and a
  `flows.dlq` topic in Kafka) so they can be inspected,
- extra information is added, such as which network zone an IP belongs to and which service a port is
  (80 = web).

**Why it's needed:** "garbage in, garbage out". The model and the reports are only as good as the
data they're given. Silver is the trusted, clean version that everything else uses.

---

## Step 7: Gold (useful summaries)

**What happens:** Spark creates ready-to-use summaries from silver:
- **attacks per minute**, for each attack type,
- **per-computer activity per minute:** how many connections each IP made and how many different
  ports it touched,
- **port-scan suspects:** any IP that touched more than 50 different ports in one minute.

**Why it's needed:** people and dashboards want answers ("who is attacking right now?"), not millions
of raw rows. Gold holds those answers, pre-calculated.

---

## Step 8: Train the model (learn what attacks look like)

**What happens:** `make train` uses **Spark MLlib** to learn from the backfill flows in silver. Two
models are trained:
- **RandomForest:** learns the patterns of each known attack type. It answers "is this an attack, and
  which kind?"
- **KMeans:** learns only what *normal* traffic looks like. Anything far away from "normal" is
  flagged as unusual.

The trained model is saved with a version number (v1, v2, and so on) in a **model registry**.

**Why it's needed:** the system can't recognise attacks without first learning from examples. Two
models are used because the RandomForest is very accurate on attacks it has seen before, while KMeans
can catch strange traffic it has never seen.

**Why Spark MLlib:** it trains on the cluster using all the data, instead of squeezing a small sample
into one computer's memory.

---

## Step 9: Score live traffic (find the attacks)

**What happens:** when the live flows arrive, Spark runs every flow through the trained models. If
either model thinks a flow is an attack, an **alert** is raised. Alerts are sent to:
- Kafka (the `alerts` topic),
- a small database (SQLite) for the dashboard,
- the gold layer (to measure accuracy later).

**Why it's needed:** this is the goal of the whole project: catching attacks while they happen. If a
newer model version is trained, Spark switches to it automatically, with no restart.

**How well it works:** 99% of alerts were real attacks, and 99% of attacks were caught.

---

## Step 10: Dashboard (show it to people)

**What happens:** `make dashboard` opens a web page (http://localhost:8501) with three tabs:

| Tab | Shows |
|---|---|
| 📡 Pipeline | how fast data is flowing, and whether anything is waiting in Kafka |
| 🚨 Alerts | how many attacks were found and how many were right, alerts per minute, and each alert marked ✅ if it really was an attack |
| 🧠 Model & hosts | how accurate the model is, the port scanners, and the most suspicious computers |

**Why it's needed:** numbers hidden in tables help nobody. The dashboard is where a security analyst
would actually see and act on the attacks.

**Why SQLite in between:** the dashboard refreshes every few seconds. Reading a small, simple
database is fast, while asking Spark each time would be slow.

---

## Example: following one attack through the whole system

Let's follow **one real kind of attack** from the dataset, step by step. On Friday afternoon in
CICIDS2017, an attacker ran a **port scan**: one computer knocks on hundreds of "doors" (ports) of
another computer to see which ones are open. Every knock is one network flow.

> The values below are simplified so they're easy to follow, but every step is exactly what the
> code does.

### 1. The flow starts as one line in a CSV file

```
Source IP: 172.16.0.1   Destination IP: 192.168.10.50   Destination Port: 22
Flow Duration: 45  ...(about 80 numbers)...  Label: PortScan
```
In plain words: computer `172.16.0.1` touched port 22 (SSH) on `192.168.10.50` for a split second.

### 2. The producer sends it into Kafka

`make live` reads that line and turns it into a message:
- **key** = `172.16.0.1` (the source IP),
- **value** = the whole row as JSON, plus `split: "live"` (this flow was hidden from training).

Because the key is the source IP, **all flows from this attacker go to the same partition**, so they
stay in order. The time is shifted to "now", so on the dashboard the attack looks like it's happening
live.

### 3. Kafka holds it

The message waits in topic `flows.raw`, say in partition 7 at position (offset) 51,204. If Spark were
busy or restarting, the message would simply wait there; nothing is lost.

### 4. `q1_bronze` saves the raw copy

Within 5 seconds, Spark picks up the message and saves it **unchanged** in the bronze table, together
with where it came from (`partition 7, offset 51204`). Spark's checkpoint now says "partition 7 is
done up to 51,204", so this message will never be read twice.

### 5. `q2_silver` cleans it

The JSON text becomes proper columns:

| Column | Value |
|---|---|
| `event_time` | 2026-10-29 14:32:05 (a real timestamp now, not text) |
| `src_ip` → `dst_ip` : `dst_port` | 172.16.0.1 → 192.168.10.50 : 22 |
| `label_family` | **Port Scanning** (the raw label `PortScan` tidied into a group) |
| `dst_zone` | internal (the victim is inside the company network) |
| `service` | SSH (port 22) |
| `flow_uid` | a unique fingerprint, so this flow can never be saved twice |

It passes all the quality checks (it has a time and a valid duration), so it goes to **silver**. A
broken record would have gone to **quarantine** instead.

### 6. Three queries read the clean flow at the same time

**`q3a_gold_timeline`** adds it to the count for that minute:
```
14:32 – 14:33   Port Scanning   flows: 1,860
```

**`q3b_gold_hosts`** adds it to this attacker's activity for that minute:
```
172.16.0.1   14:32 – 14:33   flows: 1,860   different ports touched: 980
```
980 is more than the 50-port limit, so `172.16.0.1` is listed as a **port-scan suspect**. Note that
this works by counting, even without the ML model.

**`q4_scoring`** gives the flow to the trained models:
- **RandomForest:** "this looks like Port Scanning, 97% sure". That's ≥ 60%, so it's an attack.
- **KMeans:** "this is far away from normal traffic" (distance higher than 26.85).

Either one is enough, so an **alert** is raised. It's tagged with the model version (`v1`).

### 7. The results are saved in several places

| Where | Why |
|---|---|
| Delta `gold_scored` | the permanent record of every scored flow, used later to measure accuracy |
| Kafka topic `alerts` | so any other system (an email or SMS alerter, a firewall) could react to it |
| SQLite `alerts` | a small, fast copy that the dashboard reads |

### 8. The dashboard shows it

A few seconds later, in http://localhost:8501:
- **Pipeline:** shows the batch being processed and the Kafka lag going back to 0.
- **Alerts:** the "Port Scanning" bar jumps up at 14:32, and a new row in the latest alerts says
  `172.16.0.1 → 192.168.10.50:22 · Port Scanning · 0.97` with a ✅, because the real label really
  was PortScan, so the model was right.
- **Model & hosts:** `172.16.0.1` is at the top of the port-scan suspects.

### The whole journey at a glance

```
CSV line ─► producer ─► Kafka (partition 7) ─► q1_bronze (raw copy)
   │                                              │
   │ "172.16.0.1 touched port 22"                 ▼
   │                                          q2_silver (clean columns, "Port Scanning")
   │                                    ┌─────────┼──────────────┐
   │                                    ▼         ▼              ▼
   │                               q3a timeline  q3b hosts    q4_scoring
   │                               (+1 at 14:32) (980 ports → (RF 97% → ALERT)
   │                                              suspect)       │
   │                                    └─────────┼──────────────┘
   ▼                                              ▼
 a few seconds later ──────────────────────► Dashboard: line spikes, host flagged, ✅ alert
```

From one line in a file to an alert on screen takes **a few seconds**, and the same path runs
for every one of the ~1 million flows.

### And a normal flow?

A normal web visit (say `192.168.10.5 → 8.8.8.8:443`) takes exactly the same path. The difference is
at the end: RandomForest says "Normal, 99%", and KMeans says it's close to normal traffic. So **no
alert** is raised. The flow is still counted in the timeline and host tables and still saved in
`gold_scored`.

---

## Glossary

| Term | What it means, in simple words |
|---|---|
| **Big data** | Data too large or too fast for one normal computer and program to handle comfortably |
| **Network flow** | A summary of one connection between two computers: who talked to whom, for how long, and how much data |
| **Cluster** | Several computers (here, containers) working together as a team |
| **Docker / container** | A ready-made box that holds a program plus everything it needs to run, started with one command |
| **Kafka** | A system that safely stores incoming messages in order, like a waiting line, until they're processed |
| **Topic** | A named line in Kafka, for example `flows.raw` for incoming flows or `alerts` for alerts |
| **Partition** | A topic split into smaller lines, so several workers can read at the same time |
| **Producer** | The program that sends data into Kafka |
| **Spark** | A big data engine that splits work into small tasks and runs them in parallel |
| **Structured Streaming** | Spark processing data continuously, in small batches, as it arrives |
| **Micro-batch** | One small group of new records, processed every few seconds |
| **Checkpoint** | Spark's bookmark of how far it has got, so it can continue after a crash |
| **Kappa architecture** | A design where all data enters through one stream (Kafka) and is processed only one way |
| **Data lake** | A big, cheap store of files holding all the data, raw and processed |
| **Lakehouse** | A data lake with database-like safety: no half-written data, change history, and the ability to update rows |
| **Delta Lake** | The technology that turns our data lake into a lakehouse |
| **Bronze layer** | The raw copy of the data, exactly as received |
| **Silver layer** | The cleaned, checked and trusted version of the data |
| **Gold layer** | Ready-to-use summaries and results (counts per minute, alerts) |
| **Medallion architecture** | The bronze → silver → gold pattern, where each layer is cleaner and more useful than the last |
| **Quarantine / dead-letter queue (DLQ)** | A separate place where broken records go, instead of being deleted |
| **Window** | A time slice, for example "all flows between 10:01 and 10:02", used to count things over time |
| **Watermark** | How long Spark waits for late-arriving data before it closes a time window |
| **Machine learning model** | A program that learns patterns from examples and then makes predictions on new data |
| **Spark MLlib** | Spark's machine learning library, which trains models across the cluster |
| **RandomForest** | A model made of many decision trees that vote. Here, it recognises known attack types |
| **KMeans** | A model that groups similar things. Here, it learns what "normal" looks like and flags what's far from it |
| **Model registry** | A folder of saved model versions (v1, v2, ...) so the newest can be used and older ones kept |
| **Alert** | A flow that the models think is an attack |
| **Precision** | Of all the alerts raised, how many were real attacks |
| **Recall** | Of all the real attacks, how many raised an alert |
| **Dashboard** | The web page showing what the system is doing and what it found |
