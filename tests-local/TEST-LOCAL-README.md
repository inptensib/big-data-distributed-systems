# Tests Locaux - MapReduce

Ce dossier contient les scripts nécessaires pour exécuter et tester le framework MapReduce en local sur une seule machine. Il simule un environnement distribué en lançant plusieurs processus workers sur la même machine.

## Prérequis

- Python 3.x
- Un environnement Linux/Unix

## Structure

- `deploy_local.sh` : Script principal de gestion du cycle de vie (déploiement, démarrage, arrêt).
- `download_books.sh` : Script pour télécharger le dataset de livres (Gutenberg).
- `download_commoncrawl.sh` : Script pour télécharger des données CommonCrawl (WET files).
- `.env` : Fichier de configuration (ports, nombre de workers, chemins).

## Configuration

Le fichier `.env` définit les paramètres d'exécution. Voici les principales variables à configurer :

### Chemins et Environnement
- `CODE_PYTHON_DIR` : Chemin vers le code source Python (ex: `../map-reduce`).
- `TEST_WORKSPACE` : Dossier temporaire pour l'exécution (ex: `/tmp/test_workspace`).
- `SPLITS_TARGET_DIR` : Dossier contenant les données d'entrée.
- `SPLITS_DATASET_TYPE` : Type de dataset (`books` ou `commoncrawl`).

### Paramètres du Cluster Local
- `NUM_WORKERS` : Nombre de processus workers à lancer simultanément.
- `MASTER_PORT` : Port d'écoute du Master (défaut: `8000`).
- `WORKERS_INITIAL_PORT` : Port de départ pour les workers (incrémenté pour chaque worker).

### Options d'Exécution
- `SHUFFLE_MODE` : Mode de stockage pour le shuffle (`ram` ou `disk`).
- `SHUFFLE_BATCH_SIZE` : Taille des lots de données pour le shuffle.
- `SPLIT_LIMIT` : Nombre maximum de splits à traiter (laisser vide pour tout traiter). Utile pour des tests rapides.

## Utilisation
```bash
cd tests-local;
dos2unix *;
chmod +x *.sh;
chmod +x ../map-reduce/*.py;
```

Le script `deploy_local.sh` s'utilise avec différentes commandes :

### 1. Préparation des données

Si vous n'avez pas encore de données, utilisez l'un des scripts de téléchargement :

```bash
./download_books.sh
# ou
./download_commoncrawl.sh
```

### 2. Déploiement du code

Copie les fichiers sources Python les plus récents dans l'espace de travail de test.

```bash
./deploy_local.sh deploy
```

### 3. Démarrage du Job (WordCount par défaut)

Lance le Master et les Workers. Un Job ID unique est généré (ex: `20251120_144511`).

```bash
./deploy_local.sh start
```

### 4. Suivi des Logs

Affiche les logs du Master et des Workers en temps réel.

```bash
./deploy_local.sh logs
```

### 5. Arrêt du Job

Arrête tous les processus (Master et Workers).

```bash
./deploy_local.sh stop
```

### 6. Nettoyage

Supprime les fichiers temporaires, logs et sorties des jobs précédents.

```bash
./deploy_local.sh clean
```

## Résultats

- **Logs** : Disponibles dans `tests-local/workspace/<JOB_ID>/logs/`
- **Sortie** : Les résultats du MapReduce sont dans `tests-local/workspace/<JOB_ID>/output/`
