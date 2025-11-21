# **MapReduce Distribué**

Github Repo : https://github.com/inptensib/big-data-distributed-systems

## 📋 **Vue d'ensemble**

Implémentation complète et optimisée d'un système **MapReduce distribué** en Python avec **asyncio**, conçu pour traiter des datasets massifs.

## **Structure du projet**

```
BIGDATA/
├── map-reduce/
│   ├── ybt_master_map_reduce.py    # Master  
│   ├── ybt_worker_map_reduce.py    # Worker  
│   ├── protocoldata701.py          # Protocole TCP 
├── tests-distant/
│   ├── deploy.sh                   # Déploiement cluster distant
│   └── .env                        # Configuration SSH/NFS
└── tests-local/
    ├── deploy_local.sh             # Tests localhost
    └── .env
```

---

## **Tests**

### **Test local**
Cliquez [Ici](tests-local/TEST-LOCAL-README.md) 
 
---

### **Cluster distant (Machine de l'école)**
Cliquez [Ici](tests-distant/TEST-DISTANT-README.md) 

---

## Résulats d'éxécution sur les données CommonCrawl sur les machines de l'école
Cliquez [Ici](Resultats-Execution-CommonCrawl.csv) 

