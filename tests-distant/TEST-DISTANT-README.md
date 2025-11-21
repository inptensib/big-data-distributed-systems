# Tests Distants - MapReduce

Ce dossier contient les scripts pour déployer et exécuter le framework MapReduce sur un cluster de machines distantes (salles TP). Il gère la communication SSH, le partage de fichiers via NFS, et l'orchestration des nœuds.

## Architecture

- **Master / Serveur NFS** : Une machine est élue pour agir comme Master et serveur de fichiers.
- **Workers** : Les autres machines disponibles exécutent les tâches de Map et Reduce.
- **Découverte** : Les machines disponibles sont récupérées via une API(https://tp.telecom-paris.fr/ajax.php).

## Prérequis

- Accès SSH sans mot de passe (clés SSH configurées) vers les machines cibles.
- `jq` installé (pour parser la réponse de l'API de découverte des machines).
- Pour le script d'analyse de performance (Loi d'Amdahl) : matplotlib et pandas installés dans votre environnement Python local.

## Structure

- `deploy.sh` : Script maître pour tout gérer (déploiement, exécution, analyse).
- `amdal_analyser.sh` : Script dédié à l'analyse de performance (Loi d'Amdahl).
- `analyze_amdahl.py` : Script Python générant les graphiques et rapports de performance.
- `.env` : Configuration (utilisateurs SSH, chemins, ports).

## Configuration 

Le fichier `tests-distant/.env` contient les variables essentielles pour le déploiement.

### Connexion et Infrastructure
- `SSH_USER` : Nom d'utilisateur pour la connexion SSH (ex: `boutrouft-25`).
- `REMOTE_PATH` : Chemin absolu sur le serveur NFS où le code sera déployé.
- `API_URL` : URL de l'API pour découvrir les machines disponibles.
- `DNS_SUFFIX` : Suffixe DNS des machines (ex: `.enst.fr`).

### Paramètres MapReduce
- `MASTER_PORT` : Port d'écoute du Master.
- `SHUFFLE_PORT_START` : Port de départ pour les workers sur les machines distantes.
- `SHUFFLE_MODE` : Mode de shuffle (`ram` ou `disk`).
- `BATCH_SIZE` : Taille des batchs de shuffle.

### Options de Test
- `SPLIT_LIMIT` : Limite le nombre de splits traités par défaut.
- `NUM_WORKERS_TO_LAUNCH` : Nombre de workers à lancer par défaut si non spécifié.

## Utilisation
```bash
cd tests-distant;
dos2unix *;
dos2unix .*;
chmod +x *.sh;
chmod +x ../map-reduce/*.py;
chmod +x *.py;
```
Le script `deploy.sh` est le point d'entrée unique.

### 0. Mise à jour du fichier .env
Avant de lancer des commandes, assurez-vous que le fichier `tests-distant/.env` est correctement configuré avec vos paramètres SSH, chemins, et ports.
/!\ Surtout, vérifiez que `SSH_USER` correspond à votre user SSH.
/!\ Assurez-vous que `REMOTE_PATH` est un chemin où vous avez les droits en écriture sur les machines distantes.

### 1. Récupération des machines

Avant toute chose, il faut récupérer la liste des machines disponibles via l'API et définir qui sera le Master et qui seront les Workers.

```bash
./deploy.sh retrieve-master-nfs-server-and-workers <number-of-workers>
# Exemple : ./deploy.sh retrieve-master-nfs-server-and-workers 4
```

### 2. Déploiement

Configure le serveur NFS sur le Master, monte le partage sur les Workers, et copie le code source.

```bash
./deploy.sh deploy
```

### 3. Lancer un Job (WordCount)

Lance le Master et les Workers. Vous pouvez spécifier le nombre de splits à traiter.

```bash
./deploy.sh start <number_of_splits>
# Exemple : ./deploy.sh start 5
```

### 4. Cycle Complet (Deploy + Start)

Effectue un déploiement suivi d'un démarrage.

```bash
./deploy.sh full <number_of_splits>
```

### 5. Suivi des Logs

Plusieurs commandes sont disponibles pour surveiller l'exécution :

```bash
# Logs du Master uniquement
./deploy.sh logs-master

# Récuperer les logs de fin d'exécution du Master
./deploy.sh logs-master| grep -B 1 -A 4 "RÉSUMÉ FINAL DU JOB"

# Logs de tous les Workers
./deploy.sh logs-workers

# Logs d'un Worker spécifique (via son port de shuffle)
./deploy.sh logs-worker <port>
# Exemple log du premier worker : ./deploy.sh logs-worker 9001 

# Récupérer tous les logs dans le dossier local
./deploy.sh collect-logs
```

### 6. Arrêt du Cluster

Arrête les processus Python sur toutes les machines distantes (A utiliser surtout en cas de processus fantômes pour tuer tous les processus `python` liés au projet sur les machines distantes.).

```bash
./deploy.sh stop
```

**Processus fantômes** : Utilisez `./deploy.sh stop` pour tuer tous les processus `python` liés au projet sur les machines distantes.


### 7. Résultats et Nettoyage

```bash
# Afficher ou télécharger les résultats d'éxécution (CSV)
./deploy.sh results display
./deploy.sh results download

# Télécharger les résultats de reduce à partir des workers
./deploy.sh collect-reduce-outputs


# Nettoyer les fichiers temporaires distants
./deploy.sh clean-job
```

### 8. Analyse de Performance (Loi d'Amdahl)

Lance une série de tests automatisés avec un nombre variable de workers pour générer des courbes de speedup.

```bash
# Cette commande télécharge les résultats et lance l'analyse
./deploy.sh launch-amdal-analysis

```
ou directement via le script :
```bash
./amdal_analyser.sh <csv_output_file>
# Exemple : ./amdal_analyser.sh Resultats-Execution-CommonCrawl.csv
```

## Troubleshooting

- **Problème de connexion** : Vérifiez que vos clés SSH sont bien propagées (`ssh-copy-id`).
- **Processus fantômes** : Utilisez `./deploy.sh stop` pour tuer tous les processus `python` liés au projet sur les machines distantes.
- **Ports occupés** : Assurez-vous que les ports définis dans `.env` ne sont pas utilisés par d'autres applications.