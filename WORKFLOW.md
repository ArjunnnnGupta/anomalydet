# How the Project Works: Simple Workflow

This explains the project step by step in plain words: what happens at each step, and **why** that
step is needed. Difficult terms are explained in the glossary at the end.

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

The work is done in layers: bronze → silver → gold.

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

**What happens:** `make dashboard` opens a web page (http://localhost:8501) with five tabs:

| Tab | Shows |
|---|---|
| Live pipeline | how fast data is flowing, and whether anything is waiting in Kafka |
| Threat timeline | alerts per minute, by attack type |
| Hosts | the most suspicious computers, and port scanners |
| Alert feed | each individual alert, marked ✅ if it really was an attack |
| Lakehouse & model | the data tables, and how accurate the model is |

**Why it's needed:** numbers hidden in tables help nobody. The dashboard is where a security analyst
would actually see and act on the attacks.

**Why SQLite in between:** the dashboard refreshes every few seconds. Reading a small, simple
database is fast, while asking Spark each time would be slow.

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
