# 🚀 **MapReduce Distribué Haute Performance**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20WSL2-lightgrey.svg)](https://www.linux.org/)

## 📋 **Vue d'ensemble**

Implémentation complète et optimisée d'un système **MapReduce distribué** en Python avec **asyncio**, conçu pour traiter des datasets massifs (1.2 To testés) avec une scalabilité quasi-linéaire jusqu'à 64+ workers.

### **🎯 Points clés**
- ✅ **Batching 50k tuples** → -99% appels réseau
- ✅ **Compression zlib** → Ratio 3-5x
- ✅ **Combiner RAM** → -70% shuffle
- ✅ **Multi-level merge** → Datasets > RAM
- ✅ **Fault tolerance** → Heartbeat + réassignation auto
- ✅ **Analyse Amdahl** → Export CSV + graphes

---

## 🚀 **Quick Start**

### **Test local (2 minutes)**
```bash
cd tests-local
./deploy_local.sh start 5  # 5 splits
./deploy_local.sh logs
```

### **Cluster distant (10 workers)**
```bash
cd tests-distant
./deploy.sh retrieve-master-nfs-server-and-workers 10
./deploy.sh deploy
./deploy.sh start 100  # 100 splits
./deploy.sh logs
```

---

## 📁 **Structure du projet**

```
BIGDATA/
├── map-reduce/
│   ├── ybt_master_map_reduce.py    # 👑 Master (616 lignes)
│   ├── ybt_worker_map_reduce.py    # 🤖 Worker (591 lignes, OPTIMISÉ)
│   ├── protocoldata701.py          # 📡 Protocole TCP (132 lignes)
│   └── mapreduce-{master,worker}   # 🔧 Exécutables shell
├── tests-distant/
│   ├── deploy.sh                   # 🌍 Déploiement cluster (606 lignes)
│   └── .env                        # 🔐 Configuration SSH/NFS
└── tests-local/
    ├── deploy_local.sh             # 🏠 Tests localhost
    └── .env
```

---

## ⚡ **Optimisations clés**

### **1. Combiner local (RAM mode)**
```python
# ❌ AVANT : {mot: [1, 1, 1, ...]} → RAM explosion
intermediate_data = defaultdict(list)

# ✅ APRÈS : {mot: count} → -70% RAM, -70% shuffle
intermediate_data = defaultdict(int)
intermediate_data[key] += value
```

### **2. Multi-level sort-merge (Disk mode)**
```python
# Fusion hiérarchique par niveaux (512 fichiers/niveau)
# Niveau 1: [run_1...run_512] → super_run_L1_C0.tmp
# Niveau 2: [super_run_L1_*]  → super_run_L2_C0.tmp
# → Gère milliards de tuples sans "Too many open files"
```

### **3. Batching + Compression**
```python
# Buffer 50k tuples → Compresse avec zlib → 1 appel TCP
# Gain: -99% appels réseau, -70% bande passante
await protocol.send_compressed_json(writer, batch_50k)
```

---

## 📊 **Performances (1.2 To, 3997 splits)**

| Workers | Temps | Speedup | Efficacité |
|---------|-------|---------|------------|
| 1       | ~20h  | 1.0x    | 100%       |
| 4       | ~5.2h | 3.8x    | 95%        |
| 8       | ~2.8h | 7.1x    | **89%**    |
| 16      | ~1.6h | 12.5x   | 78%        |
| 32      | ~1.0h | 20.0x   | 62%        |
| 64      | ~40m  | 30.0x   | 47%        |
| 100     | ~30m  | 40.0x   | 40%        |

**Observations** :
- ✅ Scalabilité quasi-linéaire jusqu'à **8 workers** (89%)
- ⚠️ Overhead shuffle commence à **16 workers**
- ⚠️ Saturation I/O NFS au-delà de **64 workers**

---

## 🧪 **Analyse Amdahl**

```bash
# Test 20 splits, 1-10 workers (30 min)
./deploy.sh launch-amdal-analysis 20 10

# Test 500 splits, échantillonnage log (3-4h)
./deploy.sh launch-amdal-analysis-log 500 64

# Test complet 3997 splits (3-5 jours)
./deploy.sh launch-amdal-analysis-log 3997 128
```

**Résultat** : CSV avec `num_workers`, `execution_time_sec`, `speedup`, `efficiency`

---

## 🔧 **Commandes principales**

### **Cluster distant**
```bash
./deploy.sh retrieve-master-nfs-server-and-workers 10  # Config
./deploy.sh deploy                                     # Déploie code
./deploy.sh start 100                                  # Lance job
./deploy.sh logs                                       # Logs [HOST:PORT]
./deploy.sh stop                                       # Arrête tout
./deploy.sh results                                    # CSV Amdahl
```

### **Local**
```bash
./deploy_local.sh start 20    # Lance 20 splits
./deploy_local.sh logs        # Logs temps réel
./deploy_local.sh stop        # Arrêt
```

---

## 📡 **Protocole de communication**

| Message | Direction | Description |
|---------|-----------|-------------|
| `REGISTER {port:9001}` | Worker→Master | Enregistrement initial |
| `SYNC {all_peers, your_id}` | Master→Worker | Topologie complète |
| `HEARTBEAT` | Worker→Master | Signal vie (5s) |
| `PROCESS "split.txt"` | Master→Worker | Ordre MAP |
| `DONE "split.txt"` | Worker→Master | MAP terminé |
| `REDUCE {job_id}` | Master→Worker | Ordre REDUCE |
| `REDUCE_DONE` | Worker→Master | REDUCE terminé |
| `SHUTDOWN` | Master→Worker | Arrêt propre |

**Format** : 4-byte length prefix + JSON payload (control) ou zlib(JSON) (shuffle data)

---

## 🐛 **Troubleshooting**

### **Workers ne se connectent pas**
```bash
ssh $MASTER_HOST "ps aux | grep ybt_master"
./deploy.sh logs | grep MASTER
```

### **Timeout Amdahl**
```bash
# Éditer deploy.sh ligne 590
timeout=7200  # 2h au lieu de 1h
```

### **Shuffle lent**
```bash
vim tests-distant/.env
SHUFFLE_MODE="ram"       # Au lieu de "disk"
BATCH_SIZE=200000        # Au lieu de 50000
```



---

## 📊 **Métriques**

| Métrique | Valeur |
|----------|--------|
| Lignes code Python | ~2000 |
| Dataset max | 1.2 To |
| Workers max | 128 |
| Speedup max | 40x |
| Efficacité (8 workers) | 89% |
| Réduction RAM | -70% à -90% |
| Compression shuffle | 3-5x |

---

## 📝 **Auteur**

**Younes Boutrouft** - Télécom Paris 2025  
Projet : Systèmes Distribués

---

## 🔗 **Références**

- [MapReduce (Google, 2004)](https://research.google/pubs/pub62/)
- [Loi d'Amdahl](https://fr.wikipedia.org/wiki/Loi_d%27Amdahl)
- [Python asyncio](https://docs.python.org/3/library/asyncio.html)


---

